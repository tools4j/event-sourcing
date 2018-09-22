package org.tools4j.eventsourcing.common;

import org.agrona.collections.LongLongConsumer;
import org.tools4j.eventsourcing.api.EventProcessingState;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.api.Transaction;

import java.util.Objects;

public class TransactionCommitAndPushNoops implements Poller.IndexConsumer {
    private final Transaction transaction;
    private final EventProcessingState completedUpstreamState;
    private final EventProcessingState completedDownstreamState;
    private final PushMoreUpToDateNoopSourceIds pushMoreUpToDateNoopSourceIds = new PushMoreUpToDateNoopSourceIds();

    public TransactionCommitAndPushNoops(final Transaction transaction,
                                         final EventProcessingState completedUpstreamState,
                                         final EventProcessingState completedDownstreamState) {
        this.transaction = Objects.requireNonNull(transaction);
        this.completedUpstreamState = Objects.requireNonNull(completedUpstreamState);
        this.completedDownstreamState = Objects.requireNonNull(completedDownstreamState);
    }

    @Override
    public void accept(final long index, final int source, final long sourceId, final long eventTimeNanos) {
        pushMoreUpToDateNoopSourceIds.excludedSource = source;
        pushMoreUpToDateNoopSourceIds.eventTimeNanos = eventTimeNanos;

        if (transaction.commit() > 0) {
            completedUpstreamState.forEachSourceEntry(pushMoreUpToDateNoopSourceIds);
        }
    }

    /**
     * Not thread-safe. This is a hacky way to avoid lambda capturing.
     */
    private class PushMoreUpToDateNoopSourceIds implements LongLongConsumer {
        int excludedSource;
        long eventTimeNanos;

        @Override
        public void accept(final long source, final long sourceId) {
            if (source != excludedSource && sourceId > completedDownstreamState.sourceId((int) source)) {
                transaction.init((int) source, sourceId, eventTimeNanos, true);
                transaction.commit();
            }
        }
    }
}
