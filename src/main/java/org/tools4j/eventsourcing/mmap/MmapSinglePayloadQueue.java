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
import org.tools4j.eventsourcing.api.IndexedAppender;
import org.tools4j.eventsourcing.api.IndexedQueue;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.common.SinglePayloadAppender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class MmapSinglePayloadQueue implements IndexedQueue {
    private final IndexedQueue indexedQueue;
    private final IndexedAppender appender;

    public MmapSinglePayloadQueue(final IndexedQueue indexedQueue, final int encodingBufferSize) {
        this.indexedQueue = Objects.requireNonNull(indexedQueue);

        this.appender = new SinglePayloadAppender(indexedQueue.appender(),
                new UnsafeBuffer(ByteBuffer.allocateDirect(encodingBufferSize)));
    }

    @Override
    public IndexedAppender appender() {
        return appender;
    }

    @Override
    public Poller createPoller(final Poller.Options options) throws IOException {
        return indexedQueue.createPoller(options);
    }

    @Override
    public void close() {
        indexedQueue.close();
    }
}
