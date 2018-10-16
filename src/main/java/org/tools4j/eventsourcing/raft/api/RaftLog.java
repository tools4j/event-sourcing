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

import org.agrona.DirectBuffer;
import org.tools4j.eventsourcing.sbe.RaftIndexDecoder;

import java.io.Closeable;

public interface RaftLog extends Closeable {
    long NULL_INDEX = -1;
    int NULL_TERM = 0;
    int NOT_VOTED_YET = -1;

    /**
     * Consumes a message.
     * @param term - term
     * @param source - message source
     * @param sourceSeq - sequence in the source
     * @param eventTimeNanos - time of the message
     * @param buffer - direct buffer to read message from
     * @param offset - offset of the message in the buffer
     * @param length - length of the message
     */
    void append(int term, int source, long sourceSeq, long eventTimeNanos,
                DirectBuffer buffer, int offset, int length);

    long commitIndex();

    void commitIndex(long commitIndex);

    long size();

    default long lastIndex() {
        return size() - 1;
    }

    default int lastTerm() {
        return term(lastIndex());
    }

    int term(long index);

    void wrap(final long index, final RaftIndexDecoder indexDecoder, final DirectBuffer messageBuffer);

    void truncate(long index);

    int votedFor();

    void votedFor(int serverId);

    default boolean hasNotVotedYet() {
        return votedFor() == NOT_VOTED_YET;
    }

    int currentTerm();

    void currentTerm(int term);

    default int clearVoteForAndSetCurrentTerm(final int term) {
        votedFor(NOT_VOTED_YET);
        currentTerm(term);
        return term;
    }

    default int clearVoteForAndIncCurrentTerm() {
        votedFor(NOT_VOTED_YET);
        final int incTerm = currentTerm() + 1;
        currentTerm(incTerm);
        return incTerm;
    }

    default LogContainment contains(final long index, int termAtIndex) {
        return LogContainment.containmentFor(index, termAtIndex, this);
    }

    default int lastKeyCompareTo(final long index, final int term) {
        final int termCompare = Integer.compare(lastTerm(), term);
        return termCompare == 0 ? Long.compare(lastIndex(), index) : termCompare;
    }


    @Override
    void close();
}
