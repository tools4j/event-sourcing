/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eventsourcing.core;

import org.tools4j.eventsourcing.application.ServerConfig;
import org.tools4j.eventsourcing.application.ServerContext;

import java.util.Arrays;
import java.util.Objects;

public class MutableServerContext implements ServerContext {

    private final ServerConfig config;

    private int leaderIndex = -1;
    private long lastAppendedOutputQueueIndex;
    private long lastAppliedOutputQueueIndex;
    private final long[] sourceInputSequenceNumbers;

    public MutableServerContext(final ServerConfig config) {
        this.config = Objects.requireNonNull(config);
        this.lastAppendedOutputQueueIndex = -1L;
        this.lastAppliedOutputQueueIndex = -1L;
        this.sourceInputSequenceNumbers = new long[config.inputSourceCount()];
        Arrays.fill(sourceInputSequenceNumbers, -1L);
    }

    @Override
    public ServerConfig config() {
        return config;
    }

    @Override
    public int leaderIndex() {
        return leaderIndex;
    }

    void leaderIndex(final int leaderIndex) {
        if (leaderIndex < 0 || leaderIndex >= config.serverCount()) {
            throw new IllegalArgumentException("Invalid leader index: " + leaderIndex);
        }
        this.leaderIndex = leaderIndex;
    }

    void leaderId(final int leaderId) {
        final int index = config.serverIndexOf(leaderId);
        if (index < 0) {
            throw new IllegalArgumentException("Invalid leader ID: " + leaderId);
        }
        leaderIndex(index);
    }

    @Override
    public int leaderId() {
        final int index = leaderIndex();
        if (index < 0) {
            throw new IllegalStateException("No current leader");
        }
        return config.serverId(index);
    }

    void lastAppendedOutputQueueIndex(final long index) {
        if (index < lastAppendedOutputQueueIndex) {
            throw new IllegalArgumentException("Last appended output queue index is " +
                    lastAppendedOutputQueueIndex + " so it cannot be updated to " + index);
        }
        this.lastAppendedOutputQueueIndex = index;
    }

    @Override
    public long lastAppendedOutputQueueIndex() {
        return lastAppendedOutputQueueIndex;
    }

    void lastAppliedOutputQueueIndex(final long index) {
        if (index < lastAppliedOutputQueueIndex) {
            throw new IllegalArgumentException("Last applied output queue index is " +
                    lastAppliedOutputQueueIndex + " so it cannot be updated to " + index);
        }
        this.lastAppliedOutputQueueIndex = index;
    }

    @Override
    public long lastAppliedOutputQueueIndex() {
        return lastAppliedOutputQueueIndex;
    }

    @Override
    public long lastAppliedInputSourceSeqNo(final int sourceId) {
        final int index = config.inputSourceIndexOf(sourceId);
        if (index < 0) {
            throw new IllegalArgumentException("Invalid input source ID: " + sourceId);
        }
        return sourceInputSequenceNumbers[index];
    }

    void lastAppliedSourceSeqNo(final int inputSourceId, final long sourceSeqNo) {
        final int index = config.inputSourceIndexOf(inputSourceId);
        if (index < 0) {
            throw new IllegalArgumentException("Invalid input source ID: " + inputSourceId);
        }
        if (sourceInputSequenceNumbers[index] > sourceSeqNo) {
            throw new IllegalArgumentException(
                    "Out of sequence processing of inputs: inputSourceId=" + inputSourceId +
                    " lastAppliedSourceSeqNo=" + sourceInputSequenceNumbers[index] + ", sourceSeqNo=" + sourceSeqNo
            );
        }
        sourceInputSequenceNumbers[index] = sourceSeqNo;
    }
}
