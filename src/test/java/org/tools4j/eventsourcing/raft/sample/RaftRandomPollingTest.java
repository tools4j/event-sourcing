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
package org.tools4j.eventsourcing.raft.sample;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.api.CommandExecutionQueue;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.api.ProgressState;
import org.tools4j.eventsourcing.common.PollingProcessStep;
import org.tools4j.eventsourcing.mmap.MmapBuilder;
import org.tools4j.eventsourcing.mmap.MmapTransactionalQueue;
import org.tools4j.eventsourcing.mmap.RegionRingFactoryConfig;
import org.tools4j.eventsourcing.mmap.TestUtil;
import org.tools4j.eventsourcing.raft.api.RaftQueue;
import org.tools4j.eventsourcing.raft.api.RaftQueueTest;
import org.tools4j.eventsourcing.raft.mmap.MmapRaftQueueBuilder;
import org.tools4j.eventsourcing.raft.transport.PollerFactory;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.test.*;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Service;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

public class RaftRandomPollingTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRandomPollingTest.class);

    private int clusterSize = 3;
    private int producers = 3;
    private int commandsToSend = 50;
    private int numberOfBlockings = 30;
    private boolean enableLogging = false;
    private Aeron aeron;
    private RegionRingFactory regionRingFactory;
    private String directory;

    private LongSupplier systemNanoClock;

    private List<Service> raftInstances = new ArrayList<>();
    private List<Service> commandProducers = new ArrayList<>();
    private Map<Integer, State> raftInstanceStates = new HashMap<>();
    private Map<Integer, List<Double>> raftInstanceResults = new HashMap<>();

    private Map<Integer, AtomicInteger> raftInstanceCommitted = new HashMap<>();
    private Queue<Long> blockingEventIndexes = new ArrayBlockingQueue<>(100);

    private Service createRaftInstance(final int serverId, final int clusterSize) throws IOException {
        final State instanceState = new State();
        raftInstanceStates.put(serverId, instanceState);

        final MessageHeaderDecoder commandHeaderDecoder = new MessageHeaderDecoder();
        final MessageHeaderDecoder eventHeaderDecoder = new MessageHeaderDecoder();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

        final AddCommandDecoder addCommandDecoder = new AddCommandDecoder();
        final DivideCommandDecoder divCommandDecoder = new DivideCommandDecoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
        final UpdateEventEncoder updateEncoder = new UpdateEventEncoder();
        final UpdateEventDecoder updateDecoder = new UpdateEventDecoder();
        final StringBuilder sb = new StringBuilder();
        final MutableReference<ProgressState> curEventApplyingStateRef = new MutableReference<>();
        final MutableReference<ProgressState> compEventApplyingStateRef = new MutableReference<>();

        final MessageConsumer.EventApplierFactory eventApplierFactory =
                (currentEventApplierState, completedEventApplierState) ->
                        (buf, ofst, len) -> {
                            eventHeaderDecoder.wrap(buf, ofst);
                            switch (eventHeaderDecoder.templateId()) {
                                case UpdateEventDecoder.TEMPLATE_ID:
                                    updateDecoder.wrap(buf, ofst + eventHeaderDecoder.encodedLength(),
                                            eventHeaderDecoder.blockLength(), eventHeaderDecoder.schemaId());
                                    instanceState.setValue(updateDecoder.value());

//                                    sb.setLength(0);
//                                    updateDecoder.appendTo(sb);
//                                    LOGGER.info("Applied event: {}", sb);
//                                    LOGGER.info("Current applier {}/{}", currentEventApplierState.source(), currentEventApplierState.sourceSeq());
//                                    LOGGER.info("Completed applier {}/{}", completedEventApplierState.source(), completedEventApplierState.sourceSeq());
                            }
                        };

        final MessageConsumer.CommandExecutorFactory commandExecutorFactory =
                (eventApplier,
                 currentCommandExecutionState,
                 completedCommandExecutionState,
                 currentEventApplyingState,
                 completedEventApplierState) -> {

                    curEventApplyingStateRef.set(currentEventApplyingState);
                    compEventApplyingStateRef.set(completedEventApplierState);

                    final CommandSender updateSender = value -> {
                        final int length = updateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                                .value(value)
                                .encodedLength() + headerEncoder.encodedLength();
                        eventApplier.accept(buffer, 0, length);

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

//                                sb.setLength(0);
//                                addCommandDecoder.appendTo(sb);
//                                LOGGER.info("Executed command: {}", sb);
//                                LOGGER.info("Current executor {}/{}", currentCommandExecutionState.source(), currentCommandExecutionState.sourceSeq());
//                                LOGGER.info("Completed executor {}/{}", completedCommandExecutionState.source(), completedCommandExecutionState.sourceSeq());
//                                LOGGER.info("Current applier {}/{}", currentEventApplyingState.source(), currentEventApplyingState.sourceSeq());
//                                LOGGER.info("Completed applier {}/{}", completedEventApplierState.source(), completedEventApplierState.sourceSeq());


                                break;
                            case DivideCommandDecoder.TEMPLATE_ID:
                                divCommandDecoder.wrap(buf, ofst + commandHeaderDecoder.encodedLength(),
                                        commandHeaderDecoder.blockLength(), commandHeaderDecoder.schemaId());
                                updateSender.send(instanceState.getValue() / divCommandDecoder.value());

//                                sb.setLength(0);
//                                divCommandDecoder.appendTo(sb);
//                                LOGGER.info("Executed command: {}", sb);
//                                LOGGER.info("Current executor {}/{}", currentCommandExecutionState.source(), currentCommandExecutionState.sourceSeq());
//                                LOGGER.info("Completed executor {}/{}", completedCommandExecutionState.source(), completedCommandExecutionState.sourceSeq());
//                                LOGGER.info("Current applier {}/{}", currentEventApplyingState.source(), currentEventApplyingState.sourceSeq());
//                                LOGGER.info("Completed applier {}/{}", completedEventApplierState.source(), completedEventApplierState.sourceSeq());
                        }
                    };
                };


        final String serverChannel = "aeron:ipc";
        final IntFunction<String> serverToChannel = servrId -> serverChannel;

        final LongConsumer truncateHandler = size -> {
            LOGGER.info("Truncating to size {}, last applied to state {}", size, compEventApplyingStateRef.get().id());
            //if (compEventApplyingStateRef.get().id() >= size) {
                LOGGER.info("Rebuilding the state");
                //1. comment below if pre-commit apply does not work
                curEventApplyingStateRef.get().reset();
                compEventApplyingStateRef.get().reset();
                instanceState.setValue(0);
            //}
        };

        final RaftQueue raftQueue = MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                .clusterSize(clusterSize)
                .serverId(serverId)
                .directory(directory)
                .filePrefix("raft_log")
                .regionRingFactory(regionRingFactory)
                .clearFiles(true)
                .logInMessages(enableLogging)
                .logOutMessages(enableLogging)
                .truncateHandler(truncateHandler)
                .build();

        final CommandExecutionQueue commandExecutionQueue = CommandExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_"+ serverId)
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueue(new MmapTransactionalQueue(raftQueue, 1024))
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(eventApplierFactory)
                .systemNanoClock(systemNanoClock)
                .leadership(raftQueue::leader)
                .stateChangingSource(value -> true)
                .build();

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

        final Poller resultReadingPoller = commandExecutionQueue.createPoller();
        final Step resultReadingStep = new PollingProcessStep(resultReadingPoller, resultReader);

        final Step step = combine(
                combine(combine(commandExecutionQueue.executorStep(), raftQueue.executionStep()), resultReadingStep),
                () -> {
                    if (!blockingEventIndexes.isEmpty()) {
                        final long nextBlocking = blockingEventIndexes.peek();
                        if (raftQueue.leader() && resultedEvents.get() > 0 && resultedEvents.get() == nextBlocking) {
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
                        commandExecutionQueue.appender().accept(addDecoder.source(), addDecoder.sourceSeq(), System.nanoTime(), buffer1, offset, length);
                        stringBuilder.setLength(0);
                        addDecoder.appendTo(stringBuilder);
                        LOGGER.info("Received add command: {}", stringBuilder);
                        break;
                    case DivideCommandDecoder.TEMPLATE_ID:
                        divDecoder.wrap(buffer1, offset + headerDecoder.encodedLength(),
                                headerDecoder.blockLength(), headerDecoder.schemaId());
                        commandExecutionQueue.appender().accept(divDecoder.source(), divDecoder.sourceSeq(), System.nanoTime(), buffer1, offset, length);
                        stringBuilder.setLength(0);
                        divDecoder.appendTo(stringBuilder);
                        LOGGER.info("Received div command: {}", stringBuilder);
                }
            };
            commandPollers.add(pollerFactory.create(commandReceiver , 1));
        });

        final org.tools4j.eventsourcing.raft.transport.Poller randomPoller = new RandomPoller(commandPollers);

        final Step finalStep = combine(randomPoller::poll, step);

        //this::allRaftInstancesHavePublishedEvents
        return TestUtil.startService("raftInstance" + serverId, finalStep, () -> false);
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

    private Service createCommandProducer(final int producerId) {
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
        final String aeronDirectory = directory + "/aeron";

        final MediaDriver.Context mediaDriverContex = new MediaDriver.Context();
        mediaDriverContex.aeronDirectoryName(aeronDirectory);
        final MediaDriver driver = MediaDriver.launchEmbedded(mediaDriverContex);

        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .aeronDirectoryName(aeronDirectory)
                .availableImageHandler(RaftQueueTest::printAvailableImage)
                .unavailableImageHandler(RaftQueueTest::printUnavailableImage);

        aeron = Aeron.connect(ctx);

        regionRingFactory = RegionRingFactoryConfig.get("SYNC");

        systemNanoClock = System::nanoTime;

        final Random random = new Random();
        random.longs(numberOfBlockings,0, commandsToSend * clusterSize)
                .distinct()
                .sorted()
                .forEach(blockingEventIndexes::add);


        IntStream.range(0, clusterSize).forEach(raftInstanceId -> {
            raftInstanceCommitted.put(raftInstanceId, new AtomicInteger(0));
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
        commandProducers.forEach(Service::shutdown);
        raftInstances.forEach(Service::shutdown);
    }

    @Test
    public void addInstancesShouldProduceSameEvents() throws Exception {
        commandProducers.forEach(service -> service.awaitTermination(15, TimeUnit.SECONDS));
        raftInstances.forEach(service -> service.awaitTermination(15, TimeUnit.SECONDS));
        //FIXME assert all states are the same

        raftInstanceStates.forEach((serverId, state) -> {
            LOGGER.info("Instance {} state is {}", serverId, state.getValue());
        });

        logAllResults();
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
