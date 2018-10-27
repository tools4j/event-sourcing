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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.sbe.RaftHeaderDecoder;
import org.tools4j.eventsourcing.sbe.RaftHeaderEncoder;
import org.tools4j.eventsourcing.sbe.RaftIndexDecoder;
import org.tools4j.eventsourcing.sbe.RaftIndexEncoder;

import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * Non-thread safe!
 */
public final class MmapRaftLog implements RaftLog {
    private static final long NOT_INITIALISED = -1;
    private static final int INDEX_OFFSET = 0;
    private static final int LAST_INDEX_POSITION_OFFSET = 0;
    private static final int LAST_INDEX_POSITION_LENGTH = 8;
    private static final int HEADER_OFFSET = LAST_INDEX_POSITION_OFFSET + LAST_INDEX_POSITION_LENGTH;

    private static final int INDEX_LENGTH = RaftIndexEncoder.ENCODED_LENGTH;

    private final RaftRegionAccessorSupplier regionAccessorSupplier;

    private final UnsafeBuffer mappedHeaderBuffer;
    private final UnsafeBuffer mappedIndexBuffer;
    private final UnsafeBuffer mappedMessageBuffer;

    private final RaftIndexEncoder raftIndexEncoder = new RaftIndexEncoder();
    private final RaftIndexDecoder raftIndexDecoder = new RaftIndexDecoder();
    private final RaftHeaderEncoder raftHeaderEncoder = new RaftHeaderEncoder();
    private final RaftHeaderDecoder raftHeaderDecoder = new RaftHeaderDecoder();
    private final LongConsumer truncateHandler;


    private long currentIndexPosition = NOT_INITIALISED;
    private long currentMessagePosition = 0;
    private volatile long commitIndex = NULL_INDEX;

    public MmapRaftLog(final RaftRegionAccessorSupplier regionAccessorSupplier,
                       final LongConsumer truncateHandler) {
        this.regionAccessorSupplier = Objects.requireNonNull(regionAccessorSupplier);
        this.truncateHandler = Objects.requireNonNull(truncateHandler);

        this.mappedHeaderBuffer = new UnsafeBuffer();
        this.mappedIndexBuffer = new UnsafeBuffer();
        this.mappedMessageBuffer = new UnsafeBuffer();
    }

    @Override
    public void append(final int term, final int source, final long sourceSeq, final long eventTimeNanos, final DirectBuffer buffer, final int offset, final int length) {
        init();

        if (regionAccessorSupplier.messageAccessor().wrap(currentMessagePosition, mappedMessageBuffer)) {
            if (regionAccessorSupplier.indexAccessor().wrap(currentIndexPosition, mappedIndexBuffer)) {

                if (mappedMessageBuffer.capacity() < length) {
                    currentMessagePosition += mappedMessageBuffer.capacity();
                    if (!regionAccessorSupplier.messageAccessor().wrap(currentMessagePosition, mappedMessageBuffer)) {
                        throw new IllegalStateException("Failed to wrap message buffer for position " + currentMessagePosition);
                    }
                }
                buffer.getBytes(offset, mappedMessageBuffer, 0, length);

                raftIndexEncoder.wrap(mappedIndexBuffer, INDEX_OFFSET)
                        .length(length)
                        .position(currentMessagePosition)
                        .term(term)
                        .source(source)
                        .sourceSeq(sourceSeq)
                        .eventTimeNanos(eventTimeNanos);

                advanceIndexToNextAppendPosition(length);

                mappedHeaderBuffer.putLongOrdered(LAST_INDEX_POSITION_OFFSET, currentIndexPosition);

            } else {
                throw new IllegalStateException("Failed to wrap index buffer for position " + currentIndexPosition);
            }
        } else {
            throw new IllegalStateException("Failed to wrap body buffer for position " + currentMessagePosition);
        }
    }

    private void advanceIndexToNextAppendPosition(int messageLength) {
        currentIndexPosition += INDEX_LENGTH;
        currentMessagePosition += messageLength;
    }

    private void init() {
        if (currentIndexPosition == NOT_INITIALISED) {
            if (regionAccessorSupplier.headerAccessor().wrap(0, mappedHeaderBuffer)) {
                currentIndexPosition = mappedHeaderBuffer.getLong(LAST_INDEX_POSITION_OFFSET); //does not have to be volatile
                raftHeaderEncoder.wrap(mappedHeaderBuffer, HEADER_OFFSET);
                raftHeaderDecoder.wrap(mappedHeaderBuffer, HEADER_OFFSET);

                if (currentIndexPosition > 0) {
                    if (regionAccessorSupplier.indexAccessor().wrap(currentIndexPosition - INDEX_LENGTH, mappedIndexBuffer)) {
                        raftIndexDecoder.wrap(mappedIndexBuffer, INDEX_OFFSET);
                        currentMessagePosition = raftIndexDecoder.position() + raftIndexDecoder.length();
                    } else {
                        throw new IllegalStateException("Failed to wrap index buffer to position " + (currentIndexPosition - INDEX_LENGTH));
                    }
                } else {
                    currentMessagePosition = 0;
                }
            }
        }
    }

    @Override
    public void close() {
        regionAccessorSupplier.close();
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public void commitIndex(final long commitIndex) {
        this.commitIndex = commitIndex;
    }

    @Override
    public long size() {
        init();
        return currentIndexPosition / INDEX_LENGTH; //does not have to be volatile
    }

    /**
     * no range checks
     * @param index
     */
    private void wrapIndex(long index) {
        if (!regionAccessorSupplier.indexAccessor().wrap(index * INDEX_LENGTH, mappedIndexBuffer)) {
            throw new IllegalStateException("Failed to wrap index buffer for index " + index);
        }
        raftIndexDecoder.wrap(mappedIndexBuffer, INDEX_OFFSET);
    }

    @Override
    public int term(final long index) {
        if (index > NULL_INDEX) {
            final long lastIndex = lastIndex();
            if (index > lastIndex) {
                throw new IllegalArgumentException("Index " + index + " of out last index boundary " + lastIndex);
            }
            wrapIndex(index);
            return raftIndexDecoder.term();
        } else {
            return NULL_TERM;
        }
    }

    @Override
    public void truncate(final long size) {
        final long currentSize = size();
        if (size >= 0 && size <= currentSize) {
            if (size == 0) {
                currentIndexPosition = 0;
                currentMessagePosition = 0;
                wrapIndex(0);
                raftIndexEncoder
                        .eventTimeNanos(0)
                        .length(0)
                        .position(0)
                        .source(0)
                        .sourceSeq(0)
                        .term(0);
            } else {
                currentIndexPosition = size * INDEX_LENGTH;
                wrapIndex(size - 1);
                currentMessagePosition = raftIndexDecoder.position() + raftIndexDecoder.length();
            }
            mappedHeaderBuffer.putLongOrdered(LAST_INDEX_POSITION_OFFSET, currentIndexPosition);
            truncateHandler.accept(size);
        } else {
            throw new IllegalArgumentException("Size [" + size + "] must be positive and <= current size " + currentSize);
        }
    }

    @Override
    public void wrap(final long index, final RaftIndexDecoder indexDecoder, final DirectBuffer messageBuffer) {
        final long lastIndex = lastIndex();
        if (index > NULL_INDEX && index <= lastIndex) {
            wrapIndex(index);
            indexDecoder.wrap(mappedIndexBuffer, INDEX_OFFSET);

            final long payloadPosition = indexDecoder.position();
            final int payloadLength = indexDecoder.length();
            if (regionAccessorSupplier.messageAccessor().wrap(payloadPosition, mappedMessageBuffer)) {
                messageBuffer.wrap(mappedMessageBuffer, 0, payloadLength);
            } else {
                throw new IllegalStateException("Failed to wrap payload buffer for position " + payloadPosition);
            }
        } else {
            throw new IllegalArgumentException("Index [" + index + "] must be positive and <= " + lastIndex);
        }
    }

    @Override
    public int votedFor() {
        init();
        return raftHeaderDecoder.votedFor();
    }

    @Override
    public void votedFor(final int serverId) {
        init();
        raftHeaderEncoder.votedFor(serverId);
    }

    @Override
    public int currentTerm() {
        init();
        return raftHeaderDecoder.currentTerm();
    }

    @Override
    public void currentTerm(final int term) {
        init();
        raftHeaderEncoder.currentTerm(term);
    }
}
