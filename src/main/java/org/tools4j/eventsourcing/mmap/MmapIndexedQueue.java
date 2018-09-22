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
import org.tools4j.eventsourcing.api.IndexedMessageConsumer;
import org.tools4j.eventsourcing.api.IndexedPollerFactory;
import org.tools4j.eventsourcing.api.IndexedQueue;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.common.SinglePayloadAppender;
import org.tools4j.mmap.region.api.RegionRingFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class MmapIndexedQueue implements IndexedQueue {
    private final IndexedMessageConsumer appender;
    private final IndexedPollerFactory pollerFactory;
    private final MmapIndexedAppender mmapIndexedAppender;

    public MmapIndexedQueue(final String directory,
                            final String filePrefix,
                            final boolean clearFiles,
                            final RegionRingFactory regionRingFactory,
                            final int regionSize,
                            final int regionRingSize,
                            final int regionsToMapAhead,
                            final long maxFileSize,
                            final int encodingBufferSize) throws IOException {

        this.mmapIndexedAppender = new MmapIndexedAppender(
                RegionAccessorSupplier.forReadWrite(
                        directory,
                        filePrefix,
                        clearFiles,
                        regionRingFactory,
                        regionSize,
                        regionRingSize,
                        regionsToMapAhead,
                        maxFileSize));

        this.appender = new SinglePayloadAppender(this.mmapIndexedAppender,
                new UnsafeBuffer(ByteBuffer.allocateDirect(encodingBufferSize)));

        this.pollerFactory = new MmapIndexedPollerFactory(
                directory,
                filePrefix,
                regionRingFactory,
                regionSize,
                regionRingSize,
                regionsToMapAhead);
    }

    @Override
    public IndexedMessageConsumer appender() {
        return this.appender;
    }

    @Override
    public Poller createPoller(final Poller.Options options) throws IOException {
        return pollerFactory.createPoller(options);
    }

    @Override
    public void close() {
        mmapIndexedAppender.close();
    }
}
