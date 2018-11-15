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
package org.tools4j.eventsourcing.common;

import org.agrona.collections.Long2LongHashMap;
import org.tools4j.eventsourcing.api.IndexConsumer;
import org.tools4j.eventsourcing.api.ProgressState;

import java.util.Objects;
import java.util.function.LongSupplier;

public final class DefaultProgressState implements ProgressState, IndexConsumer {
    private static final long MISSING_VALUE = -1;
    private final Long2LongHashMap sourceSeqMap;
    private final LongSupplier systemNanoClock;

    private long id = NOT_INITIALISED;
    private int source = NOT_INITIALISED;
    private long sourceSeq = NOT_INITIALISED;
    private long eventTimeNanos = NOT_INITIALISED;
    private long ingestionTimeNanos = NOT_INITIALISED;
    private boolean commandPollerResetRequired = false;
    private boolean eventPollerResetRequired = false;

    public DefaultProgressState(final LongSupplier systemNanoClock) {
        this.systemNanoClock = Objects.requireNonNull(systemNanoClock);
        this.sourceSeqMap = new Long2LongHashMap(MISSING_VALUE);
        this.eventTimeNanos = 0;
    }

    @Override
    public long sourceSeq(final int source) {
        return sourceSeqMap.get(source);
    }

    @Override
    public void accept(final long id, final int source, final long sourceSeq, final long eventTimeNanos) {
        sourceSeqMap.put(source, sourceSeq);
        this.id = id;
        this.source = source;
        this.sourceSeq = sourceSeq;
        this.eventTimeNanos = eventTimeNanos;
        this.ingestionTimeNanos = systemNanoClock.getAsLong();
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public int source() {
        return source;
    }

    @Override
    public long sourceSeq() {
        return sourceSeq;
    }

    @Override
    public long eventTimeNanos() {
        return eventTimeNanos;
    }

    @Override
    public long ingestionTimeNanos() {
        return ingestionTimeNanos;
    }

    @Override
    public void reset() {
        id = NOT_INITIALISED;
        source = NOT_INITIALISED;
        sourceSeq = NOT_INITIALISED;
        eventTimeNanos = NOT_INITIALISED;
        ingestionTimeNanos = NOT_INITIALISED;
        sourceSeqMap.clear();
        commandPollerResetRequired = true;
        eventPollerResetRequired = true;
    }

    @Override
    public boolean commandPollerResetRequired(final long currentPosition) {
        return commandPollerResetRequired;
    }

    @Override
    public boolean eventPollerResetRequired(final long currentPosition) {
        return eventPollerResetRequired;
    }

    @Override
    public void resetCommandPoller(final long resetPosition) {
        commandPollerResetRequired = false;
    }

    @Override
    public void resetEventPoller(final long resetPosition) {
        eventPollerResetRequired = false;
    }

}
