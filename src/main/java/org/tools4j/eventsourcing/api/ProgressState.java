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
package org.tools4j.eventsourcing.api;

import org.agrona.collections.LongLongConsumer;

/**
 * Progress State provides event details at different stages of processing of the command/event.
 * There are the following stages when progress state can be observed:
 * - current command progress
 * - completed command progress
 * - current event progress
 * - completed event progress
 */
public interface ProgressState {
    int NOT_INITIALISED = -1;

    /**
     * @return Position of the event/command in the queue. Starts from 0 and increments by 1 (for each event).
     */
    long id();

    /**
     * @return Source of the event/command.
     */
    int source();

    /**
     * @return sequence within the source.
     */
    long sourceSeq();

    /**
     * @param source given source
     * @return sourceSeq of the event/command from the given source progressed far.
     */
    long sourceSeq(int source);

    /**
     * @return time of the event/command in nanos
     */
    long eventTimeNanos();

    /**
     * @return system time when the event/command is ingested at current stage.
     */
    long ingestionTimeNanos();

    /**
     * For each source entry containing last sourceSeq.
     * @param consumer to run
     */
    void forEachSourceEntry(LongLongConsumer consumer);

    void reset();
    boolean pollerResetRequired(long currentPosition);
    void resetPoller(long resetPosition);

    boolean isBehind(final ProgressState another);

    boolean isBehind(final int source, ProgressState another);

    default boolean isEqualTo(final ProgressState another) {
        return sourceSeq() == another.sourceSeq() &&
                source() == another.source();
    }

    default boolean isNotEqualTo(final ProgressState another) {
        return !isEqualTo(another);
    }
}
