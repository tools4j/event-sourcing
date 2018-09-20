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

import java.io.Closeable;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Queue poller
 */
public interface Poller extends Closeable {
    /**
     * polls a queue and invokes the consumer if a message is available for consumption.
     * @param consumer of a polled message if available
     * @return number of polled messages
     */
    int poll(MessageConsumer consumer);

    @Override
    default void close(){}

    /**
     * Tests index details
     */
    interface IndexPredicate {
        IndexPredicate NEVER = (index, source, sourceId, eventTimeNanos) -> false;
        IndexPredicate ALWAYS = (index, source, sourceId, eventTimeNanos) -> true;


        boolean test(long index, int source, long sourceId, long eventTimeNanos);

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * AND of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code false}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ANDed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * AND of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default IndexPredicate and(final IndexPredicate other) {
            Objects.requireNonNull(other);
            return (i, s, sid, etn) -> test(i, s, sid, etn) && other.test(i, s, sid, etn);
        }

        /**
         * Returns a predicate that represents the logical negation of this
         * predicate.
         *
         * @return a predicate that represents the logical negation of this
         * predicate
         */
        default IndexPredicate negate() {
            return (i, s, sid, etn) -> !test(i, s, sid, etn);
        }

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * OR of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code true}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ORed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * OR of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default IndexPredicate or(final IndexPredicate other) {
            Objects.requireNonNull(other);
            return (i, s, sid, etn) -> test(i, s, sid, etn) || other.test(i, s, sid, etn);
        }

        static IndexPredicate isLessThanOrEqual(final EventProcessingState eventProcessingState) {
            return (index, source, sourceId, eventTimeNanos) -> sourceId <= eventProcessingState.sourceId(source);
        }

        static IndexPredicate isLessThan(final EventProcessingState eventProcessingState) {
            return (index, source, sourceId, eventTimeNanos) -> sourceId < eventProcessingState.sourceId(source);
        }

        static IndexPredicate isEqualTo(final EventProcessingState eventProcessingState) {
            return (index, source, sourceId, eventTimeNanos) -> sourceId == eventProcessingState.sourceId() && source == eventProcessingState.source();
        }

        static IndexPredicate isGreaterThan(final EventProcessingState eventProcessingState) {
            return isLessThanOrEqual(eventProcessingState).negate();
        }

        static IndexPredicate eventTimeBefore(final long timeNanos) {
            return (index, source, sourceId, eventTimeNanos) -> eventTimeNanos < timeNanos;
        }

        static IndexPredicate never() {
            return NEVER;
        }

        static IndexPredicate always() {
            return ALWAYS;
        }

        static IndexPredicate isTrue(final BooleanSupplier booleanSupplier) {
            return (index, source, sourceId, eventTimeNanos) -> booleanSupplier.getAsBoolean();
        }

        static IndexPredicate isLeader(final BooleanSupplier leadership) {
            return IndexPredicate.isTrue(leadership);
        }

        static IndexPredicate isNotLeader(final BooleanSupplier leadership) {
            return isLeader(leadership).negate();
        }
    }

    /**
     * Index consumer
     */
    interface IndexConsumer {
        IndexConsumer NO_OP = (index, source, sourceId, eventTimeNanos) -> {};
        void accept(long index, int source, long sourceId, long eventTimeNanos);

        default IndexConsumer andThen(final IndexConsumer after) {
            Objects.requireNonNull(after);
            return (i, s, sid, etn) -> { accept(i, s, sid, etn); after.accept(i, s, sid, etn); };
        }

        static IndexConsumer transactionInit(final Transaction transaction) {
            return (index, source, sourceId, eventTimeNanos) -> transaction.init(source, sourceId, eventTimeNanos);
        }

        static IndexConsumer transactionCommit(final Transaction transaction) {
            return (index, source, sourceId, eventTimeNanos) -> transaction.commit();
        }

        static IndexConsumer noop() {
            return NO_OP;
        }
    }

    interface Options {
        IndexPredicate skipWhen();
        IndexPredicate pauseWhen();
        IndexConsumer onProcessingStart();
        IndexConsumer onProcessingComplete();
        IndexConsumer onProcessingSkipped();

        interface Builder {
            Builder skipWhen(IndexPredicate skipWhen);
            Builder pauseWhen(IndexPredicate pauseWhen);
            Builder onProcessingStart(IndexConsumer onProcessingStart);
            Builder onProcessingComplete(IndexConsumer onProcessingComplete);
            Builder onProcessingSkipped(IndexConsumer onProcessingSkipped);
            Options build();
        }

        static Builder builder() {
            return new Builder() {
                private IndexPredicate skipWhen = IndexPredicate.never();
                private IndexPredicate pauseWhen = IndexPredicate.never();
                private IndexConsumer onProcessingStart = IndexConsumer.noop();
                private IndexConsumer onProcessingComplete = IndexConsumer.noop();
                private IndexConsumer onProcessingSkipped = IndexConsumer.noop();

                @Override
                public Builder skipWhen(final IndexPredicate skipWhen) {
                    this.skipWhen = skipWhen;
                    return this;
                }

                @Override
                public Builder pauseWhen(final IndexPredicate pauseWhen) {
                    this.pauseWhen = pauseWhen;
                    return this;
                }

                @Override
                public Builder onProcessingStart(final IndexConsumer onProcessingStart) {
                    this.onProcessingStart = onProcessingStart;
                    return this;
                }

                @Override
                public Builder onProcessingComplete(final IndexConsumer onProcessingComplete) {
                    this.onProcessingComplete = onProcessingComplete;
                    return this;
                }

                @Override
                public Builder onProcessingSkipped(final IndexConsumer onProcessingSkipped) {
                    this.onProcessingSkipped = onProcessingSkipped;
                    return this;
                }

                @Override
                public Options build() {
                    return new Options() {
                        @Override
                        public IndexPredicate skipWhen() {
                            return skipWhen;
                        }

                        @Override
                        public IndexPredicate pauseWhen() {
                            return pauseWhen;
                        }

                        @Override
                        public IndexConsumer onProcessingStart() {
                            return onProcessingStart;
                        }

                        @Override
                        public IndexConsumer onProcessingComplete() {
                            return onProcessingComplete;
                        }

                        @Override
                        public IndexConsumer onProcessingSkipped() {
                            return onProcessingSkipped;
                        }
                    };
                }
            };
        }
    }
}
