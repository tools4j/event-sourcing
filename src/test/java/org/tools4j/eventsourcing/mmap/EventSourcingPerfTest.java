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
package org.tools4j.eventsourcing.mmap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.MetricIndexConsumer;
import org.tools4j.eventsourcing.TestMessage;
import org.tools4j.eventsourcing.api.ExecutionQueue;
import org.tools4j.eventsourcing.api.IndexPredicate;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.common.PollingProcessStep;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Step;
import org.tools4j.nobark.loop.StoppableThread;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

public class EventSourcingPerfTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventSourcingPerfTest.class);

    public static void main(String... args) throws Exception {

        final long messagesPerSecond = 100000;
        final long maxNanosPerMessage = 1000000000 / messagesPerSecond;
        final int messages = 1000000;
        final int warmup = 500000;
        final AtomicBoolean stop = new AtomicBoolean(false);

        final RegionRingFactory regionRingFactory = TestUtil.getRegionRingFactory(args);

        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        final MessageConsumer senderMessageConsumer = (buffer, offset, length) -> {};

        final ExecutionQueue queue = ExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("event")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .commandExecutorFactory(
                        (eventApplier,
                         currentProgressState,
                         completedProgressState) -> eventApplier)
                .eventApplierFactory(
                        (currentProgressState, completedProgressState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .build();

        final Poller senderPoller = queue.createPoller(
                Poller.Options.builder()
                        .skipWhen(
                            IndexPredicate.eventTimeBefore(systemNanoClock.getAsLong()))
                        .onProcessingComplete(
                            new MetricIndexConsumer(messages, warmup, stop))
                        .build()
        );

        final Step senderStep = new PollingProcessStep(senderPoller, senderMessageConsumer);

        final StoppableThread eventProcessor = TestUtil.startService("event-processor", queue.executorStep(), stop::get);
        final StoppableThread sender = TestUtil.startService("event-sender", senderStep, stop::get);

        final TestMessage message = TestMessage.forLength(300);

        final long seed = System.currentTimeMillis();

        LOGGER.info("Start sourceSeq {}", seed);

        for (int i = 0; i < messages; i++) {
            final long start = System.nanoTime();
            queue.appender().accept(1, seed + i, start, message.buffer, message.offset, message.length);
            long end = System.nanoTime();
            final long waitUntil = start + maxNanosPerMessage;
            while (end < waitUntil) {
                end = System.nanoTime();
            }
        }

        LOGGER.info("End sourceSeq {}", seed + messages - 1);

        eventProcessor.join(60000);
        sender.join(60000);

        queue.close();
        senderPoller.close();
    }
}