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
package org.tools4j.eventsourcing.application;

import java.util.Arrays;
import java.util.Objects;

public class MutableServerContext implements ServerContext {

    private final ServerConfig config;

    private int leaderIndex = -1;
    private final long[] sourceSequenceNumbers;

    public MutableServerContext(final ServerConfig config) {
        this.config = Objects.requireNonNull(config);
        this.sourceSequenceNumbers = new long[config.inputSourceCount()];
        Arrays.fill(sourceSequenceNumbers, -1L);
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

    @Override
    public long lastAppliedSourceSeqNo(final int inputSourceId) {
        final int index = config.inputSourceIndexOf(inputSourceId);
        if (index < 0) {
            throw new IllegalArgumentException("Invalid input source ID: " + inputSourceId);
        }
        return sourceSequenceNumbers[index];
    }

    void lastAppliedSourceSeqNo(final int inputSourceId, final long sourceSeqNo) {
        final int index = config.inputSourceIndexOf(inputSourceId);
        if (index < 0) {
            throw new IllegalArgumentException("Invalid input source ID: " + inputSourceId);
        }
        if (sourceSequenceNumbers[index] > sourceSeqNo) {
            throw new IllegalArgumentException(
                    "Out of sequence processing of inputs: inputSourceId=" + inputSourceId +
                    " lastAppliedSourceSeqNo=" + sourceSequenceNumbers[index] + ", sourceSeqNo=" + sourceSeqNo
            );
        }
        sourceSequenceNumbers[index] = sourceSeqNo;
    }
}
