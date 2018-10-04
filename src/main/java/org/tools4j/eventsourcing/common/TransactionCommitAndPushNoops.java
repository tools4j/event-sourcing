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

import org.agrona.collections.LongLongConsumer;
import org.tools4j.eventsourcing.api.ProgressState;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.api.Transaction;

import java.util.Objects;

public class TransactionCommitAndPushNoops implements Poller.IndexConsumer {
    private final Transaction transaction;
    private final ProgressState completedCommandExecutionState;
    private final ProgressState completedEventApplyingState;
    private final PushMoreUpToDateNoopSourceSeqs pushMoreUpToDateNoopSourceSeqs = new PushMoreUpToDateNoopSourceSeqs();

    public TransactionCommitAndPushNoops(final Transaction transaction,
                                         final ProgressState completedCommandExecutionState,
                                         final ProgressState completedEventApplyingState) {
        this.transaction = Objects.requireNonNull(transaction);
        this.completedCommandExecutionState = Objects.requireNonNull(completedCommandExecutionState);
        this.completedEventApplyingState = Objects.requireNonNull(completedEventApplyingState);
    }

    @Override
    public void accept(final long index, final int source, final long sourceSeq, final long eventTimeNanos) {
        pushMoreUpToDateNoopSourceSeqs.excludedSource = source;
        pushMoreUpToDateNoopSourceSeqs.eventTimeNanos = eventTimeNanos;

        if (transaction.commit() > 0) {
            completedCommandExecutionState.forEachSourceEntry(pushMoreUpToDateNoopSourceSeqs);
        }
    }

    /**
     * Not thread-safe. This is a hacky way to avoid lambda capturing.
     */
    private class PushMoreUpToDateNoopSourceSeqs implements LongLongConsumer {
        int excludedSource;
        long eventTimeNanos;

        @Override
        public void accept(final long source, final long sourceSeq) {
            if (source != excludedSource && sourceSeq > completedEventApplyingState.sourceSeq((int) source)) {
                transaction.init((int) source, sourceSeq, eventTimeNanos, true);
                transaction.commit();
            }
        }
    }
}
