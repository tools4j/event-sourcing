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

import org.tools4j.eventsourcing.common.TransactionCommitAndPushNoops;

import java.util.Objects;

/**
 * Index consumer
 */
public interface IndexConsumer {
    IndexConsumer NO_OP = (index, source, sourceSeq, eventTimeNanos) -> {};
    void accept(long index, int source, long sourceSeq, long eventTimeNanos);

    default IndexConsumer andThen(final IndexConsumer after) {
        Objects.requireNonNull(after);
        return (i, s, sid, etn) -> { accept(i, s, sid, etn); after.accept(i, s, sid, etn); };
    }

    static IndexConsumer transactionInit(final Transaction transaction) {
        return (index, source, sourceSeq, eventTimeNanos) -> transaction.init(source, sourceSeq, eventTimeNanos, true);
    }

    static IndexConsumer transactionCommitAndPushNoops(final Transaction transaction) {
        return new TransactionCommitAndPushNoops(transaction);
    }

    static IndexConsumer noop() {
        return NO_OP;
    }
}
