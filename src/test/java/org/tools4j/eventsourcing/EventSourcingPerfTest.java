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
package org.tools4j.eventsourcing;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.api.EventProcessingQueue;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.config.RegionRingFactoryConfig;
import org.tools4j.eventsourcing.queue.DefaultEventProcessingQueue;
import org.tools4j.eventsourcing.queue.DefaultIndexedQueue;
import org.tools4j.eventsourcing.queue.DefaultIndexedTransactionalQueue;
import org.tools4j.eventsourcing.step.*;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.mmap.region.impl.MappedFile;
import org.tools4j.nobark.loop.Service;
import org.tools4j.nobark.loop.Step;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public class EventSourcingPerfTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventSourcingPerfTest.class);

    public static void main(String... args) throws Exception {

        final long messagesPerSecond = 40000;
        final long maxNanosPerMessage = 1000000000 / messagesPerSecond;
        final int messages = 1000000;
        final int warmup = 200000;
        final AtomicBoolean stop = new AtomicBoolean(false);


        final int regionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 1024 * 4;//64 KB
        LOGGER.info("regionSize: {}", regionSize);

        final RegionRingFactory regionRingFactory = TestUtil.getRegionRingFactory(args);

        final int ringSize = 4;
        final int regionsToMapAhead = 1;
        final long maxFileSize = 64L * 16 * 1024 * 1024 * 4;
        final int encodingBufferSize = 8 * 1024;
        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;
        final BooleanSupplier leadership = () -> true;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        final MessageConsumer senderMessageConsumer = (buffer, offset, length) -> {};

        final EventProcessingQueue queue = new DefaultEventProcessingQueue(
                new DefaultIndexedQueue(
                        directory,
                        "upstream",
                        true,
                        regionRingFactory,
                        regionSize,
                        ringSize,
                        regionsToMapAhead,
                        maxFileSize,
                        encodingBufferSize),
                new DefaultIndexedTransactionalQueue(
                        directory,
                        "downstream",
                        true,
                        regionRingFactory,
                        regionSize,
                        ringSize,
                        regionsToMapAhead,
                        maxFileSize,
                        encodingBufferSize),
                systemNanoClock,
                leadership,
                Poller.IndexConsumer.noop(),
                Poller.IndexConsumer.noop(),
                Poller.IndexConsumer.noop(),
                Poller.IndexConsumer.noop(),
                (downstreamAppender, upstreamBeforeState, downstreamAfterState) -> downstreamAppender,
                (upstreamBeforeState, downstreamAfterState) -> stateMessageConsumer,
                DownstreamWhileDoneThenUpstreamUntilDoneStep::new
        );

        final Poller senderPoller = queue.createPoller(
                Poller.IndexPredicate.eventTimeBefore(systemNanoClock.getAsLong()),
                Poller.IndexPredicate.never(),
                Poller.IndexConsumer.noop(),
                new MetricIndexConsumer(messages, warmup, stop));

        final Step senderStep = new PollingProcessStep(senderPoller, senderMessageConsumer);

        regionRingFactory.onComplete();

        final Service eventProcessor = TestUtil.startService("event-processor", queue.processorStep(), stop::get);
        final Service sender = TestUtil.startService("event-sender", senderStep, stop::get);

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(0, testMessage.getBytes());

        final int size = testMessage.getBytes().length;

        final long seed = System.currentTimeMillis();

        LOGGER.info("Start sourceId {}", seed);

        for (int i = 0; i < messages; i++) {
            final long start = System.nanoTime();
            queue.appender().accept(1, seed + i, start, unsafeBuffer, 0, size);
            long end = System.nanoTime();
            final long waitUntil = start + maxNanosPerMessage;
            while (end < waitUntil) {
                end = System.nanoTime();
            }
        }

        LOGGER.info("End sourceId {}", seed + messages - 1);

        eventProcessor.awaitTermination(1, TimeUnit.MINUTES);
        sender.awaitTermination(1, TimeUnit.MINUTES);

        queue.close();
        senderPoller.close();
    }
}