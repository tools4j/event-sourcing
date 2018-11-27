/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j, Marco Terzer, Anton Anufriev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tools4j.eventsourcing.raft.mmap;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.api.*;
import org.tools4j.eventsourcing.common.PayloadBufferPoller;
import org.tools4j.eventsourcing.common.PollingProcessStep;
import org.tools4j.eventsourcing.mmap.MmapBuilder;
import org.tools4j.eventsourcing.mmap.TestUtil;
import org.tools4j.eventsourcing.raft.api.RaftQueueTest;
import org.tools4j.eventsourcing.raft.transport.PollerFactory;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.test.*;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Step;
import org.tools4j.nobark.loop.Stoppable;
import org.tools4j.nobark.loop.StoppableThread;
import org.tools4j.spockito.Spockito;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Spockito.class)
@Spockito.Unroll({
        "| clusterSize  |  producers | commandsToSend  | numberOfBlockings  |",
        "|--------------|------------|-----------------|--------------------|",
        "|       3      |      3     |     50          | 20                 |",
        "|       5      |      3     |     20          | 10                 |",
        "|       3      |     10     |     35          | 20                 |",
        "|       3      |     10     |     1000        | 0                  |",
})
@Spockito.Name("[{row}]: cluster:{clusterSize}, producers:{producers}, commands:{commandsToSend}, blockings:{numberOfBlockings}")
public class RaftRandomPollingTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRandomPollingTest.class);

    private final int clusterSize;
    private final int producers;
    private final int commandsToSend;
    private final int numberOfBlockings;
    private final boolean enableLogging = false;
    private final int expectedEvents;
    private final int noopInterval = 10;

    private Aeron aeron;
    private MediaDriver driver;
    private String aeronDirectory;
    private String queuesDirectory;
    private RegionRingFactory regionRingFactory;
    private String directory;

    private LongSupplier systemNanoClock;

    private List<StoppableThread> raftInstances = new ArrayList<>();
    private List<StoppableThread> commandProducers = new ArrayList<>();
    private Map<Integer, State> raftInstanceStates = new HashMap<>();
    private Map<Integer, List<Double>> raftInstanceResults = new HashMap<>();

    private Map<Integer, AtomicInteger> raftInstanceCommitted = new HashMap<>();
    private Queue<Long> blockingEventIndexes = new ArrayBlockingQueue<>(100);
    private Map<Integer, AtomicBoolean> raftInstanceComplete = new HashMap<>();
    private Queue<Closeable> queuesToClose;


    public RaftRandomPollingTest(final int clusterSize,
                                 final int producers,
                                 final int commandsToSend,
                                 final int numberOfBlockings) {
        this.clusterSize = clusterSize;
        this.producers = producers;
        this.commandsToSend = commandsToSend;
        this.numberOfBlockings = numberOfBlockings;

        this.expectedEvents = producers * commandsToSend - producers * commandsToSend / noopInterval;
    }

    private boolean isNoop(final long sequence) {
        return sequence % noopInterval == 0;
    }

    private StoppableThread createRaftInstance(final int serverId, final int clusterSize) throws IOException {
        final State instanceState = raftInstanceStates.get(serverId);

        final MessageHeaderDecoder commandHeaderDecoder = new MessageHeaderDecoder();
        final MessageHeaderDecoder eventHeaderDecoder = new MessageHeaderDecoder();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

        final AddCommandDecoder addCommandDecoder = new AddCommandDecoder();
        final DivideCommandDecoder divCommandDecoder = new DivideCommandDecoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
        final UpdateEventEncoder updateEncoder = new UpdateEventEncoder();
        final UpdateEventDecoder updateDecoder = new UpdateEventDecoder();
        final StringBuilder sb = new StringBuilder();
        final MutableReference<ProgressState> curProgressStateRef = new MutableReference<>();
        final MutableReference<ProgressState> compProgressStateRef = new MutableReference<>();

        final AtomicBoolean complete = raftInstanceComplete.get(serverId);

        final EventApplierFactory eventApplierFactory =
                (currentProgressState, completedProgressState) ->
                        (buf, ofst, len) -> {
                            eventHeaderDecoder.wrap(buf, ofst);
                            switch (eventHeaderDecoder.templateId()) {
                                case UpdateEventDecoder.TEMPLATE_ID:
                                    updateDecoder.wrap(buf, ofst + eventHeaderDecoder.encodedLength(),
                                            eventHeaderDecoder.blockLength(), eventHeaderDecoder.schemaId());
                                    instanceState.setValue(updateDecoder.value());
                            }
                        };

        final CommandExecutorFactory commandExecutorFactory =
                (eventApplier,
                 currentProgressState,
                 completedProgressState) -> {

                    curProgressStateRef.set(currentProgressState);
                    compProgressStateRef.set(completedProgressState);

                    final CommandSender updateSender = value -> {
                        final int length = updateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                                .value(value)
                                .encodedLength() + headerEncoder.encodedLength();

                        if (isNoop(currentProgressState.sourceSeq())) {
                            instanceState.setValue(value);
                        } else {
                            eventApplier.accept(buffer, 0, length);
                        }

                        sb.setLength(0);
                        updateEncoder.appendTo(sb);

                        LOGGER.info("Sent update command: {}", sb);
                    };

                    return (buf, ofst, len) -> {
                        commandHeaderDecoder.wrap(buf, ofst);
                        switch (commandHeaderDecoder.templateId()) {
                            case AddCommandDecoder.TEMPLATE_ID:
                                addCommandDecoder.wrap(buf, ofst + commandHeaderDecoder.encodedLength(),
                                        commandHeaderDecoder.blockLength(), commandHeaderDecoder.schemaId());
                                updateSender.send(instanceState.getValue() + addCommandDecoder.value());

                                break;
                            case DivideCommandDecoder.TEMPLATE_ID:
                                divCommandDecoder.wrap(buf, ofst + commandHeaderDecoder.encodedLength(),
                                        commandHeaderDecoder.blockLength(), commandHeaderDecoder.schemaId());
                                updateSender.send(instanceState.getValue() / divCommandDecoder.value());
                        }
                    };
                };


        final String serverChannel = "aeron:ipc";
        final IntFunction<String> serverToChannel = servrId -> serverChannel;

        final ExecutionQueue executionQueue = ExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(queuesDirectory)
                                .filePrefix("command_"+ serverId)
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueueFactory(
                        onStateReset -> MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                            .clusterSize(clusterSize)
                            .serverId(serverId)
                            .directory(queuesDirectory)
                            .filePrefix("raft_log")
                            .regionRingFactory(regionRingFactory)
                            .clearFiles(true)
                            .logInMessages(enableLogging)
                            .logOutMessages(enableLogging)
                            .truncateHandler(size -> onStateReset.run())
                            .build()
                )
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(eventApplierFactory)
                .onStateReset(() -> instanceState.setValue(0))
                .systemNanoClock(systemNanoClock)
                .build();

        queuesToClose.add(executionQueue);

        final AtomicInteger resultedEvents = raftInstanceCommitted.get(serverId);
        final List<Double> results = new ArrayList<>();
        raftInstanceResults.put(serverId, results);

        final MessageHeaderDecoder resultEventHeaderDecoder = new MessageHeaderDecoder();
        final UpdateEventDecoder resultUpdateDecoder = new UpdateEventDecoder();

        final MessageConsumer resultReader = (buf, ofst, len) -> {
            resultEventHeaderDecoder.wrap(buf, ofst);
            switch (resultEventHeaderDecoder.templateId()) {
                case UpdateEventDecoder.TEMPLATE_ID:
                    resultUpdateDecoder.wrap(buf, ofst + resultEventHeaderDecoder.encodedLength(),
                            resultEventHeaderDecoder.blockLength(), resultEventHeaderDecoder.schemaId());
                    results.add(resultUpdateDecoder.value());
                    LOGGER.info("done committed: {}", resultedEvents.incrementAndGet());
            }
        };

        final Poller resultReadingPoller = executionQueue.createPoller(
                Poller.Options.builder()
                        .bufferPoller(new PayloadBufferPoller())
                        .build());

        final Step resultReadingStep = new PollingProcessStep(resultReadingPoller, resultReader);

        final Step step = combine(
                combine(executionQueue.executorStep(), resultReadingStep),
                () -> {
                    if (!complete.get()) {
                        if(results.size() == expectedEvents) {
                            complete.set(true);
                        }
                    }

                    if (!blockingEventIndexes.isEmpty()) {
                        final long nextBlocking = blockingEventIndexes.peek();
                        if (executionQueue.leader() && resultedEvents.get() > 0 && resultedEvents.get() == nextBlocking) {
                            try {
                                LOGGER.info("Blocking .... {}", resultedEvents.get());
                                blockingEventIndexes.poll();
                                Thread.sleep(2505);
                            } catch (InterruptedException e) {
                            }
                            return true;
                        }
                    }
                    return false;
                })
        ;

        final List<org.tools4j.eventsourcing.raft.transport.Poller> commandPollers = new ArrayList<>();
        IntStream.range(100, 100 + producers).forEach(producerId -> {
            final PollerFactory pollerFactory = PollerFactory.aeronPollerFactory(aeron, "aeron:ipc", producerId);

            final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
            final AddCommandDecoder addDecoder = new AddCommandDecoder();
            final DivideCommandDecoder divDecoder = new DivideCommandDecoder();
            final StringBuilder stringBuilder = new StringBuilder();

            final MessageConsumer commandReceiver = (buffer1, offset, length) -> {
                headerDecoder.wrap(buffer1, offset);
                switch (headerDecoder.templateId()) {
                    case AddCommandDecoder.TEMPLATE_ID:
                        addDecoder.wrap(buffer1, offset + headerDecoder.encodedLength(),
                                headerDecoder.blockLength(), headerDecoder.schemaId());
                        executionQueue.appender().accept(addDecoder.source(), addDecoder.sourceSeq(), System.nanoTime(), buffer1, offset, length);
                        stringBuilder.setLength(0);
                        addDecoder.appendTo(stringBuilder);
                        LOGGER.info("Received add command: {}", stringBuilder);
                        break;
                    case DivideCommandDecoder.TEMPLATE_ID:
                        divDecoder.wrap(buffer1, offset + headerDecoder.encodedLength(),
                                headerDecoder.blockLength(), headerDecoder.schemaId());
                        executionQueue.appender().accept(divDecoder.source(), divDecoder.sourceSeq(), System.nanoTime(), buffer1, offset, length);
                        stringBuilder.setLength(0);
                        divDecoder.appendTo(stringBuilder);
                        LOGGER.info("Received div command: {}", stringBuilder);
                }
            };
            commandPollers.add(pollerFactory.create(commandReceiver , 1));
        });

        final org.tools4j.eventsourcing.raft.transport.Poller randomPoller = new RandomPoller(commandPollers);

        final Step finalStep = combine(randomPoller::poll, step);

        return TestUtil.startService("raftInstance" + serverId, finalStep, this::allMatch);
    }

    private boolean allMatch() {
        return raftInstanceComplete.values().stream().allMatch(AtomicBoolean::get) &&
                raftInstanceStates.values().stream().mapToDouble(State::getValue).distinct().limit(2).count() == 1;
    }

    private static Step combine(final Step step1, final Step step2) {
        return () -> step1.perform() | step2.perform();
    }

    private void logAllResults() {
        for (int i = 0; i < clusterSize; i++) {
            final List<Double> results = raftInstanceResults.get(i);
            System.out.println("server:"+ i + " " + results);
        }
    }

    private StoppableThread createCommandProducer(final int producerId) {
        final AtomicLong sequence = new AtomicLong(0);
        final Publisher publisher = Publisher.aeronPublisher(aeron, "aeron:ipc", producerId);
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
        final AddCommandEncoder addEncoder = new AddCommandEncoder();
        final DivideCommandEncoder divEncoder = new DivideCommandEncoder();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

        final StringBuilder stringBuilder = new StringBuilder();

        final CommandSender addSender = value -> {
            final int length = addEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                    .source(producerId)
                    .sourceSeq(sequence.incrementAndGet())
                    .value(value)
                    .encodedLength() + headerEncoder.encodedLength();

            stringBuilder.setLength(0);
            addEncoder.appendTo(stringBuilder);
            LOGGER.info("Sent command: {}", stringBuilder);

            publisher.publish(buffer, 0, length);
        };

        final CommandSender divSender = value -> {
            final int length = divEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                    .source(producerId)
                    .sourceSeq(sequence.incrementAndGet())
                    .value(value)
                    .encodedLength() + headerEncoder.encodedLength();


            stringBuilder.setLength(0);
            divEncoder.appendTo(stringBuilder);
            LOGGER.info("Sent command: {}", stringBuilder);

            publisher.publish(buffer, 0, length);
        };

        final Random random = new Random();

        final CommandSender randomCommandSender = value -> {
            if (random.nextBoolean()) {
                addSender.send(value);
            } else {
                divSender.send(value);
            }
        };

        final Step createAndSendNewCommand = () -> {
            randomCommandSender.send(random.nextInt());
            return true;
        };

        return TestUtil.startService("producer" + producerId, createAndSendNewCommand, () -> sequence.get() == commandsToSend);
    }

    interface CommandSender {
        void send(double value);
    }

    @Before
    public void setUp() throws Exception {
        directory = System.getProperty("user.dir") + "/build";
        aeronDirectory = directory + "/aeron";
        queuesDirectory = directory + "/queues";

        IoUtil.ensureDirectoryIsRecreated(new File(aeronDirectory), "aeron", (s, s2) -> {});
        IoUtil.ensureDirectoryIsRecreated(new File(queuesDirectory), "queues", (s, s2) -> {});

        final MediaDriver.Context mediaDriverContex = new MediaDriver.Context();
        mediaDriverContex.aeronDirectoryName(aeronDirectory);
        driver = MediaDriver.launchEmbedded(mediaDriverContex);

        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .aeronDirectoryName(aeronDirectory)
                .availableImageHandler(RaftQueueTest::printAvailableImage)
                .unavailableImageHandler(RaftQueueTest::printUnavailableImage);

        aeron = Aeron.connect(ctx);

        regionRingFactory = RegionRingFactory.async();

        systemNanoClock = System::nanoTime;

        queuesToClose = new ArrayBlockingQueue<>(clusterSize);

        final Random random = new Random();
        random.longs(numberOfBlockings,0, commandsToSend * clusterSize)
                .distinct()
                .sorted()
                .forEach(blockingEventIndexes::add);


        IntStream.range(0, clusterSize).forEach(raftInstanceId -> {
            raftInstanceCommitted.put(raftInstanceId, new AtomicInteger(0));
            raftInstanceComplete.put(raftInstanceId, new AtomicBoolean(false));
            raftInstanceStates.put(raftInstanceId, new State());

        });

        IntStream.range(0, clusterSize).forEach(raftInstanceId -> {
            try {
                raftInstances.add(createRaftInstance(raftInstanceId, clusterSize));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        IntStream.range(100, 100 + producers).forEach(producerId -> {
            commandProducers.add(createCommandProducer(producerId));
        });
    }

    @After
    public void tearDown() throws Exception {
        commandProducers.forEach(Stoppable::stop);
        raftInstances.forEach(Stoppable::stop);
        queuesToClose.forEach(closeable -> {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        aeron.close();
        driver.close();
        Thread.sleep(1000);
    }

    @Test
    public void allInstancesShouldProduceSameEvents() throws Exception {
        raftInstances.forEach(service -> service.join(10000));
        commandProducers.forEach(StoppableThread::stop);

        raftInstanceStates.forEach((serverId, state) -> {
            LOGGER.info("Instance {} state is {}", serverId, state.getValue());
        });

        logAllResults();

        assertThat(allMatch()).isTrue();
    }

    class State {
        double value;

        public double getValue() {
            return value;
        }

        public void setValue(final double value) {
            this.value = value;
        }
    }
}
