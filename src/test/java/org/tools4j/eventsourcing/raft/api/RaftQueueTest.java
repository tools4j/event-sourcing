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
package org.tools4j.eventsourcing.raft.api;


import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.TestMessage;
import org.tools4j.eventsourcing.api.CommandExecutionQueue;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.common.PayloadBufferPoller;
import org.tools4j.eventsourcing.mmap.MmapBuilder;
import org.tools4j.eventsourcing.mmap.MmapTransactionalQueue;
import org.tools4j.eventsourcing.mmap.RegionRingFactoryConfig;
import org.tools4j.eventsourcing.mmap.TestUtil;
import org.tools4j.eventsourcing.raft.mmap.MmapRaftQueueBuilder;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Service;
import org.tools4j.nobark.loop.Step;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

public class RaftQueueTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftQueueTest.class);
    private static final int NON_STATE_CHANGING_SOURCE_LOW = 10000000;

    public static void main(String... args) throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);

        final MediaDriver driver = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .availableImageHandler(RaftQueueTest::printAvailableImage)
                .unavailableImageHandler(RaftQueueTest::printUnavailableImage);

        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        final Aeron aeron = Aeron.connect(ctx);

        final RegionRingFactory regionRingFactory = RegionRingFactoryConfig.get("SYNC");

        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;

        final MessageConsumer.CommandExecutorFactory commandExecutorFactory =
                (eventApplier,
                 currentCommandExecutionState,
                 completedCommandExecutionState,
                 currentEventApplyingState,
                 completedEventApplierState) -> eventApplier;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        final String serverChannel = "aeron:ipc";
        final IntFunction<String> serverToChannel = serverId -> serverChannel;

        final RaftQueue raftQueue0 = MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                .clusterSize(3)
                .serverId(0)
                .directory(directory)
                .filePrefix("raft_log")
                .regionRingFactory(regionRingFactory)
                .clearFiles(true)
                .logInMessages(false)
                .logOutMessages(false)
                .bufferPoller(new PayloadBufferPoller())
                .build();

        final RaftQueue raftQueue1 = MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                .clusterSize(3)
                .serverId(1)
                .directory(directory)
                .filePrefix("raft_log")
                .regionRingFactory(regionRingFactory)
                .clearFiles(true)
                .logInMessages(false)
                .logOutMessages(false)
                .bufferPoller(new PayloadBufferPoller())
                .build();

        final RaftQueue raftQueue2 = MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                .clusterSize(3)
                .serverId(2)
                .directory(directory)
                .filePrefix("raft_log")
                .regionRingFactory(regionRingFactory)
                .clearFiles(true)
                .logInMessages(false)
                .logOutMessages(false)
                .bufferPoller(new PayloadBufferPoller())
                .build();

        final CommandExecutionQueue commandExecutionQueue0 = CommandExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_0")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueue(new MmapTransactionalQueue(raftQueue0, 1024 * 8))
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentEventApplierState, completedEventApplierState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .leadership(raftQueue0::leader)
                .stateChangingSource(value -> value < NON_STATE_CHANGING_SOURCE_LOW)
                .build();

        final CommandExecutionQueue commandExecutionQueue1 = CommandExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_1")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueue(new MmapTransactionalQueue(raftQueue1, 1024 * 8))
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentEventApplierState, completedEventApplierState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .leadership(raftQueue1::leader)
                .stateChangingSource(value -> value < NON_STATE_CHANGING_SOURCE_LOW)
                .build();

        final CommandExecutionQueue commandExecutionQueue2 = CommandExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_2")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueue(new MmapTransactionalQueue(raftQueue2, 1024 * 8))
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentEventApplierState, completedEventApplierState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .leadership(raftQueue2::leader)
                .stateChangingSource(value -> value < NON_STATE_CHANGING_SOURCE_LOW)
                .build();

        regionRingFactory.onComplete();

        raftQueue0.init();
        raftQueue1.init();
        raftQueue2.init();


        final Step step0 = combine(commandExecutionQueue0.executorStep(), raftQueue0.executionStep());
        final Step step1 = combine(commandExecutionQueue1.executorStep(), raftQueue1.executionStep());
        final Step step2 = combine(commandExecutionQueue2.executorStep(), raftQueue2.executionStep());

        final Poller poller0 = commandExecutionQueue0.createPoller(Poller.Options.builder().build());
        final Poller poller1 = commandExecutionQueue1.createPoller(Poller.Options.builder().build());
        final Poller poller2 = commandExecutionQueue2.createPoller(Poller.Options.builder().build());

        final MessageConsumer logger = (buffer, offset, length) -> {
            LOGGER.info("POLLED EVENT: " + buffer.getStringWithoutLengthAscii(offset, length));
        };

        final Service service0 = TestUtil.startService("service0", step0, stop::get);
        final Service service1 = TestUtil.startService("service1", step1, stop::get);
        final Service service2 = TestUtil.startService("service2", step2, stop::get);

        final Service pollingService0 = TestUtil.startService("pollingService0", () -> poller0.poll(logger) > 0, stop::get);
        final Service pollingService1 = TestUtil.startService("pollingService1", () -> poller1.poll(logger) > 0, stop::get);
        final Service pollingService2 = TestUtil.startService("pollingService2", () -> poller2.poll(logger) > 0, stop::get);


        final TestMessage message1 = TestMessage.forString("Test Message 1");
        final TestMessage message2 = TestMessage.forString("Test Message 2");
        final TestMessage message3 = TestMessage.forString("Test Message 3");

        final int source1 = 10;

        final LongSupplier timeSupplier = System::nanoTime;

        Thread.sleep(20000);

        commandExecutionQueue0.appender().accept(source1, 1, timeSupplier.getAsLong(), message1.buffer, message1.offset, message1.length);
        commandExecutionQueue1.appender().accept(source1, 2, timeSupplier.getAsLong(), message2.buffer, message2.offset, message2.length);
        commandExecutionQueue2.appender().accept(source1, 3, timeSupplier.getAsLong(), message3.buffer, message3.offset, message3.length);

        commandExecutionQueue0.appender().accept(source1, 2, timeSupplier.getAsLong(), message2.buffer, message2.offset, message2.length);
        commandExecutionQueue1.appender().accept(source1, 1, timeSupplier.getAsLong(), message1.buffer, message1.offset, message1.length);
        commandExecutionQueue2.appender().accept(source1, 3, timeSupplier.getAsLong(), message3.buffer, message3.offset, message3.length);

        commandExecutionQueue0.appender().accept(source1, 3, timeSupplier.getAsLong(), message3.buffer, message3.offset, message3.length);
        commandExecutionQueue1.appender().accept(source1, 2, timeSupplier.getAsLong(), message2.buffer, message2.offset, message2.length);
        commandExecutionQueue2.appender().accept(source1, 1, timeSupplier.getAsLong(), message1.buffer, message1.offset, message1.length);

        Thread.sleep(20000000);
        stop.set(true);

        commandExecutionQueue0.close();
        commandExecutionQueue1.close();
        commandExecutionQueue2.close();
        raftQueue0.close();
        raftQueue1.close();
        raftQueue2.close();
        aeron.close();
    }

    private static Step combine(final Step step1, final Step step2) {
        return () -> step1.perform() | step2.perform();
    }

    public static void printAvailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.println(String.format(
                "Available image on %s streamId=%d sessionId=%d from %s",
                subscription.channel(), subscription.streamId(), image.sessionId(), image.sourceIdentity()));
    }

    /**
     * Print the information for an unavailable image to stdout.
     *
     * @param image that has gone inactive
     */
    public static void printUnavailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.println(String.format(
                "Unavailable image on %s streamId=%d sessionId=%d",
                subscription.channel(), subscription.streamId(), image.sessionId()));
    }

}