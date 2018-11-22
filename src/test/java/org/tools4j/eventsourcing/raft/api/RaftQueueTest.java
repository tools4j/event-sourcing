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
import org.tools4j.eventsourcing.api.CommandExecutorFactory;
import org.tools4j.eventsourcing.api.ExecutionQueue;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.common.PayloadBufferPoller;
import org.tools4j.eventsourcing.mmap.MmapBuilder;
import org.tools4j.eventsourcing.mmap.TestUtil;
import org.tools4j.eventsourcing.raft.mmap.MmapRaftQueueBuilder;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Step;
import org.tools4j.nobark.loop.StoppableThread;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

public class RaftQueueTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftQueueTest.class);

    public static void main(String... args) throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);

        final MediaDriver driver = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .availableImageHandler(RaftQueueTest::printAvailableImage)
                .unavailableImageHandler(RaftQueueTest::printUnavailableImage);

        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        final Aeron aeron = Aeron.connect(ctx);

        final RegionRingFactory regionRingFactory = RegionRingFactory.sync();

        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;

        final CommandExecutorFactory commandExecutorFactory =
                (eventApplier,
                 currentProgressState,
                 completedProgressState) -> eventApplier;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        final String serverChannel = "aeron:ipc";
        final IntFunction<String> serverToChannel = serverId -> serverChannel;

        final ExecutionQueue executionQueue0 = ExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_0")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueueFactory(
                        onStateReset -> MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                            .clusterSize(3)
                            .serverId(0)
                            .directory(directory)
                            .filePrefix("raft_log")
                            .regionRingFactory(regionRingFactory)
                            .clearFiles(true)
                            .logInMessages(false)
                            .logOutMessages(false)
                            .build())
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentProgressState, completedProgressState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .build();

        final ExecutionQueue executionQueue1 = ExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_1")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueueFactory(
                        onStateReset -> MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                            .clusterSize(3)
                            .serverId(1)
                            .directory(directory)
                            .filePrefix("raft_log")
                            .regionRingFactory(regionRingFactory)
                            .clearFiles(true)
                            .logInMessages(false)
                            .logOutMessages(false)
                            .build())
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentProgressState, completedProgressState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .build();

        final ExecutionQueue executionQueue2 = ExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command_2")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueueFactory(
                        onStateReset -> MmapRaftQueueBuilder.forAeronTransport(aeron, serverToChannel)
                            .clusterSize(3)
                            .serverId(2)
                            .directory(directory)
                            .filePrefix("raft_log")
                            .regionRingFactory(regionRingFactory)
                            .clearFiles(true)
                            .logInMessages(false)
                            .logOutMessages(false)
                            .build())
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentProgressState, completedProgressState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .build();

        executionQueue0.init();
        executionQueue1.init();
        executionQueue2.init();


        final Step step0 = executionQueue0.executorStep();
        final Step step1 = executionQueue1.executorStep();
        final Step step2 = executionQueue2.executorStep();

        final Poller poller0 = executionQueue0.createPoller(Poller.Options.builder().bufferPoller(new PayloadBufferPoller()).build());
        final Poller poller1 = executionQueue1.createPoller(Poller.Options.builder().bufferPoller(new PayloadBufferPoller()).build());
        final Poller poller2 = executionQueue2.createPoller(Poller.Options.builder().bufferPoller(new PayloadBufferPoller()).build());

        final MessageConsumer logger = (buffer, offset, length) -> {
            LOGGER.info("POLLED EVENT: " + buffer.getStringWithoutLengthAscii(offset, length));
        };

        final StoppableThread service0 = TestUtil.startService("service0", step0, stop::get);
        final StoppableThread service1 = TestUtil.startService("service1", step1, stop::get);
        final StoppableThread service2 = TestUtil.startService("service2", step2, stop::get);

        final StoppableThread pollingService0 = TestUtil.startService("pollingService0", () -> poller0.poll(logger) > 0, stop::get);
        final StoppableThread pollingService1 = TestUtil.startService("pollingService1", () -> poller1.poll(logger) > 0, stop::get);
        final StoppableThread pollingService2 = TestUtil.startService("pollingService2", () -> poller2.poll(logger) > 0, stop::get);


        final TestMessage message1 = TestMessage.forString("Test Message 1");
        final TestMessage message2 = TestMessage.forString("Test Message 2");
        final TestMessage message3 = TestMessage.forString("Test Message 3");

        final int source1 = 10;

        final LongSupplier timeSupplier = System::nanoTime;

        Thread.sleep(20000);

        executionQueue0.appender().accept(source1, 1, timeSupplier.getAsLong(), message1.buffer, message1.offset, message1.length);
        executionQueue1.appender().accept(source1, 2, timeSupplier.getAsLong(), message2.buffer, message2.offset, message2.length);
        executionQueue2.appender().accept(source1, 3, timeSupplier.getAsLong(), message3.buffer, message3.offset, message3.length);

        executionQueue0.appender().accept(source1, 2, timeSupplier.getAsLong(), message2.buffer, message2.offset, message2.length);
        executionQueue1.appender().accept(source1, 1, timeSupplier.getAsLong(), message1.buffer, message1.offset, message1.length);
        executionQueue2.appender().accept(source1, 3, timeSupplier.getAsLong(), message3.buffer, message3.offset, message3.length);

        executionQueue0.appender().accept(source1, 3, timeSupplier.getAsLong(), message3.buffer, message3.offset, message3.length);
        executionQueue1.appender().accept(source1, 2, timeSupplier.getAsLong(), message2.buffer, message2.offset, message2.length);
        executionQueue2.appender().accept(source1, 1, timeSupplier.getAsLong(), message1.buffer, message1.offset, message1.length);

        Thread.sleep(20000000);
        stop.set(true);

        executionQueue0.close();
        executionQueue1.close();
        executionQueue2.close();
        aeron.close();
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