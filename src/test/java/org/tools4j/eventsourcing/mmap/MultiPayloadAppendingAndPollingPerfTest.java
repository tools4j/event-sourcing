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

import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.MetricIndexConsumer;
import org.tools4j.eventsourcing.TestMessage;
import org.tools4j.eventsourcing.api.*;
import org.tools4j.eventsourcing.common.MultiPayloadAppender;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.mmap.region.impl.MappedFile;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiPayloadAppendingAndPollingPerfTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiPayloadAppendingAndPollingPerfTest.class);

    public static void main(String... args) throws Exception {
        final String directory =  System.getProperty("java.io.tmpdir");

        final long messagesPerSecond = 100000;
        final long maxNanosPerMessage = 1000000000 / messagesPerSecond;
        final int messages = 2000000;
        final int warmup = 200000;
        final AtomicBoolean stop = new AtomicBoolean(false);

        final int regionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 1024 * 4;
        LOGGER.info("regionSize: {}", regionSize);

        final RegionRingFactory regionRingFactory = TestUtil.getRegionRingFactory(args);

        final int ringSize = 4;
        final int regionsToMapAhead = 1;
        final long maxFileSize = 64L * 16 * 1024 * 1024 * 4;

        final Transaction appender = new MultiPayloadAppender(
                new MmapIndexedAppender(
                    RegionAccessorSupplier.forReadWriteClear(
                        directory,
                        "multiPayloadTest",
                        regionRingFactory,
                        regionSize,
                        ringSize,
                        regionsToMapAhead,
                        maxFileSize)
                ),
                new UnsafeBuffer(ByteBuffer.allocateDirect(8096)));

        final Poller poller = new MmapIndexedPoller(
                RegionAccessorSupplier.forReadOnly(
                        directory,
                        "multiPayloadTest",
                        regionRingFactory,
                        regionSize,
                        ringSize,
                        regionsToMapAhead),
                Poller.Options.builder()
                        .onProcessingStart(new MetricIndexConsumer(messages, warmup, stop))
                        .build()
        );

        final TestMessage message = TestMessage.forDefaultLength();

        final MessageConsumer messageConsumer = (buffer, offset, length) -> {};

        final Thread pollerThread = new Thread(() -> {
            while (!stop.get()) {
                poller.poll(messageConsumer);
            }
        });
        pollerThread.setName("async-processor");
        pollerThread.setDaemon(true);
        pollerThread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("{}", e));
        pollerThread.start();


        for (int i = 0; i < messages; i++) {
            final long start = System.nanoTime();
            appender.init(1,1, start, false);
            appender.accept(message.buffer, message.offset, message.length);
            //appender.accept(unsafeBuffer, 0, size);
            appender.commit();
            long end = System.nanoTime();
            final long waitUntil = start + maxNanosPerMessage;
            while (end < waitUntil) {
                end = System.nanoTime();
            }
        }

        pollerThread.join();
    }
}