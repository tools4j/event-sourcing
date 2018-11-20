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
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/**
 * Queue poller
 */
public interface Poller extends Closeable {
    /**
     * polls the queue and invokes the consumer if a message is available for consumption.
     * @param consumer of a polled message if available
     * @return number of polled messages
     */
    int poll(MessageConsumer consumer);

    @Override
    default void close(){}

    interface Options {
        IndexPredicate skipWhen();
        LongPredicate resetWhen();
        IndexConsumer onProcessingStart();
        IndexConsumer onProcessingComplete();
        IndexConsumer onProcessingSkipped();
        IndexPredicate pauseWhen();
        LongConsumer onReset();
        BufferPoller bufferPoller();

        interface Builder {
            Builder skipWhen(IndexPredicate skipWhen);
            Builder pauseWhen(IndexPredicate pauseWhen);
            Builder onProcessingStart(IndexConsumer onProcessingStart);
            Builder onProcessingComplete(IndexConsumer onProcessingComplete);
            Builder onProcessingSkipped(IndexConsumer onProcessingSkipped);
            Builder resetWhen(LongPredicate resetWhen);
            Builder onReset(LongConsumer onReset);
            Builder bufferPoller(BufferPoller bufferPoller);
            Options build();
        }

        static Builder builder() {
            return new Builder() {
                private IndexPredicate skipWhen = IndexPredicate.never();
                private IndexPredicate pauseWhen = IndexPredicate.never();
                private LongPredicate resetWhen = currentPosition -> false;
                private IndexConsumer onProcessingStart = IndexConsumer.noop();
                private IndexConsumer onProcessingComplete = IndexConsumer.noop();
                private IndexConsumer onProcessingSkipped = IndexConsumer.noop();
                private LongConsumer onReset = resetPosition -> {};
                private BufferPoller bufferPoller = BufferPoller.PASS_THROUGH;


                @Override
                public Builder skipWhen(final IndexPredicate skipWhen) {
                    this.skipWhen = Objects.requireNonNull(skipWhen);
                    return this;
                }

                @Override
                public Builder pauseWhen(final IndexPredicate pauseWhen) {
                    this.pauseWhen = Objects.requireNonNull(pauseWhen);
                    return this;
                }

                @Override
                public Builder resetWhen(final LongPredicate resetWhen) {
                    this.resetWhen = Objects.requireNonNull(resetWhen);
                    return this;
                }

                @Override
                public Builder onProcessingStart(final IndexConsumer onProcessingStart) {
                    this.onProcessingStart = Objects.requireNonNull(onProcessingStart);
                    return this;
                }

                @Override
                public Builder onProcessingComplete(final IndexConsumer onProcessingComplete) {
                    this.onProcessingComplete = Objects.requireNonNull(onProcessingComplete);
                    return this;
                }

                @Override
                public Builder onProcessingSkipped(final IndexConsumer onProcessingSkipped) {
                    this.onProcessingSkipped = Objects.requireNonNull(onProcessingSkipped);
                    return this;
                }

                @Override
                public Builder onReset(final LongConsumer onReset) {
                    this.onReset = Objects.requireNonNull(onReset);
                    return this;
                }

                @Override
                public Builder bufferPoller(final BufferPoller bufferPoller) {
                    this.bufferPoller = Objects.requireNonNull(bufferPoller);
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
                        public LongPredicate resetWhen() {
                            return resetWhen;
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

                        @Override
                        public LongConsumer onReset() {
                            return onReset;
                        }

                        @Override
                        public BufferPoller bufferPoller() {
                            return bufferPoller;
                        }
                    };
                }
            };
        }
    }
}
