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

import org.agrona.DirectBuffer;
import org.tools4j.eventsourcing.api.*;
import org.tools4j.mmap.region.api.RegionRingFactory;

import java.io.IOException;

public final class MmapReadOnlyIndexedQueue implements IndexedQueue {
    private final IndexedAppender appender;
    private final IndexedPollerFactory pollerFactory;

    public MmapReadOnlyIndexedQueue(final String directory,
                                    final String filePrefix,
                                    final RegionRingFactory regionRingFactory,
                                    final int regionSize,
                                    final int regionRingSize,
                                    final int regionsToMapAhead) throws IOException {

        this.appender = new IndexedAppender() {
            @Override
            public long lastSourceSeq(final int source) {
                return -1;
            }

            @Override
            public void accept(final int source, final long sourceSeq, final long eventTimeNanos, final DirectBuffer buffer, final int offset, final int length) {
                throw new UnsupportedOperationException("append operation is not supported");
            }
        };

        this.pollerFactory = new MmapIndexedPollerFactory(
                directory,
                filePrefix,
                regionRingFactory,
                regionSize,
                regionRingSize,
                regionsToMapAhead);
    }

    @Override
    public IndexedAppender appender() {
        return this.appender;
    }

    @Override
    public Poller createPoller(final Poller.Options options) throws IOException {
        return pollerFactory.createPoller(options);
    }
}
