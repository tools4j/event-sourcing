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

import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.sbe.RaftIndexDecoder;

import java.util.Objects;

public class MmapRaftPoller implements Poller {
    private static final int INDEX_OFFSET = 0;
    //private static final int INDEX_LENGTH = RaftIndexDecoder.ENCODED_LENGTH;
    private static final int INDEX_LENGTH = 64;

    private final RaftRegionAccessorSupplier regionAccessorSupplier;

    private final UnsafeBuffer headerBuffer;
    private final UnsafeBuffer mappedIndexBuffer;
    private final UnsafeBuffer mappedMessageBuffer;

    private final Options options;


    private final RaftIndexDecoder raftIndexDecoder = new RaftIndexDecoder();

    private long currentIndex = 0;
    private long currentIndexPosition = 0;

    public MmapRaftPoller(final RaftRegionAccessorSupplier regionAccessorSupplier,
                          final Options options) {
        this.regionAccessorSupplier = Objects.requireNonNull(regionAccessorSupplier);
        this.options = Objects.requireNonNull(options);

        this.headerBuffer = new UnsafeBuffer();
        this.mappedIndexBuffer = new UnsafeBuffer();
        this.mappedMessageBuffer = new UnsafeBuffer();
    }

    @Override
    public int poll(final MessageConsumer processingHandler) {
        if (!regionAccessorSupplier.headerAccessor().wrap(0, headerBuffer)) {
            throw new IllegalStateException("Failed to wrap header buffer");
        }

        if (options.resetWhen().test(currentIndex)) {
            final long indexBeforeReset = currentIndex;
            currentIndex = 0;
            currentIndexPosition = 0;
            options.onReset().accept(indexBeforeReset);
        }

        final long lastIndexPosition = headerBuffer.getLongVolatile(0);
        if (currentIndexPosition >= lastIndexPosition) {
            return 0;
        }
        wrapIndex();

        final int messageLength = raftIndexDecoder.length();
        if (messageLength <= 0) return 0;

        final long messagePosition = raftIndexDecoder.position();
        final int source = raftIndexDecoder.source();
        final long sourceSeq = raftIndexDecoder.sourceSeq();
        final long eventTimeNanos = raftIndexDecoder.eventTimeNanos();

        if (source == 0) {
            //skip Noop
            advanceIndexToNextAppendPosition();
            return 1;
        }

        if (options.skipWhen().test(currentIndex, source, sourceSeq, eventTimeNanos)) {
            options.onProcessingSkipped().accept(currentIndex, source, sourceSeq, eventTimeNanos);
            advanceIndexToNextAppendPosition();
            return 0;
        } else if (!options.pauseWhen().test(currentIndex, source, sourceSeq, eventTimeNanos)) {
            final int done = pollMessages(messagePosition, messageLength,  source, sourceSeq, eventTimeNanos, processingHandler);
            advanceIndexToNextAppendPosition();
            return done;
        } else {
            return 0;
        }
    }

    private int pollMessages(final long messagePosition, final int messageLength, final int source, final long sourceSeq, final long eventTimeNanos,
                             final MessageConsumer processingHandler) {
        if (!regionAccessorSupplier.messageAccessor().wrap(messagePosition, mappedMessageBuffer)) {
            throw new IllegalStateException("Failed to wrap message buffer to position " + messagePosition);
        }
        options.onProcessingStart().accept(currentIndex, source, sourceSeq, eventTimeNanos);
        final int done = options.bufferPoller().poll(mappedMessageBuffer, 0, messageLength, processingHandler);
        options.onProcessingComplete().accept(currentIndex, source, sourceSeq, eventTimeNanos);
        return done;
    }

    private void wrapIndex() {
        if (!regionAccessorSupplier.indexAccessor().wrap(currentIndexPosition, mappedIndexBuffer)) {
            throw new IllegalStateException("Failed to wrap index buffer to position " + currentIndexPosition);
        }
        raftIndexDecoder.wrap(mappedIndexBuffer, INDEX_OFFSET);
    }

    private void advanceIndexToNextAppendPosition() {
        currentIndex++;
        currentIndexPosition += INDEX_LENGTH;
    }

    @Override
    public void close() {
        regionAccessorSupplier.close();
    }
}
