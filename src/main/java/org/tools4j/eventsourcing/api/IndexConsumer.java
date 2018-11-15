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
