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

import org.agrona.collections.MutableReference;
import org.tools4j.eventsourcing.api.*;

import java.io.IOException;
import java.util.Objects;

public final class BranchedIndexedTransactionalQueue implements IndexedTransactionalQueue {
    private final IndexedPollerFactory basePollerFactory;
    private final IndexedTransactionalQueue branchQueue;
    private final IndexPredicate branchPredicate;

    public BranchedIndexedTransactionalQueue(final IndexedPollerFactory basePollerFactory,
                                             final IndexedTransactionalQueue branchQueue,
                                             final IndexPredicate branchPredicate) {
        this.basePollerFactory = Objects.requireNonNull(basePollerFactory);
        this.branchQueue = Objects.requireNonNull(branchQueue);
        this.branchPredicate = Objects.requireNonNull(branchPredicate);
    }

    @Override
    public Transaction appender() {
        return branchQueue.appender();
    }

    @Override
    public Poller createPoller(final Poller.Options options) throws IOException {
        final Poller branchQueuePoller = branchQueue.createPoller(options);
        final MutableReference<Poller> currentPollerRef = new MutableReference<>();

        final IndexPredicate needToSwitchToBranch = (index, source, sourceSeq, eventTimeNanos) -> {
            if (this.branchPredicate.test(index, source, sourceSeq, eventTimeNanos)) {
                currentPollerRef.set(branchQueuePoller);
                return true;
            } else {
                return false;
            }
        };

        final Poller firstLegPoller = basePollerFactory.createPoller(
                Poller.Options.builder()
                        .skipWhen(needToSwitchToBranch.or(options.skipWhen()))
                        .pauseWhen(options.pauseWhen())
                        .onProcessingStart(options.onProcessingStart())
                        .onProcessingComplete(options.onProcessingComplete())
                        .bufferPoller(options.bufferPoller())
                        .build()
        );

        currentPollerRef.set(firstLegPoller);

        return new Poller() {
            @Override
            public int poll(final MessageConsumer consumer) {
                return currentPollerRef.get().poll(consumer);
            }

            @Override
            public void close() {
                branchQueuePoller.close();
                firstLegPoller.close();
            }
        };
    }

    @Override
    public void close() {
        branchQueue.close();
    }

    public static Builder builder() {
        return new BranchedIndexedTransactionalQueueBuilder();
    }

    public interface Builder {
        BranchQueueStep basePollerFactory(IndexedPollerFactory basePollerFactory);

        interface BranchQueueStep {
            BranchPredicateStep branchQueue(IndexedTransactionalQueue branchQueue);
        }

        interface BranchPredicateStep {
            BuildStep branchPredicate(IndexPredicate branchPredicate);
        }

        interface BuildStep {
            IndexedTransactionalQueue build();
        }
    }

    private static class BranchedIndexedTransactionalQueueBuilder implements Builder, Builder.BranchQueueStep, Builder.BranchPredicateStep, Builder.BuildStep {
        IndexedPollerFactory basePollerFactory;
        IndexedTransactionalQueue branchQueue;
        IndexPredicate branchPredicate;

        @Override
        public BranchQueueStep basePollerFactory(final IndexedPollerFactory basePollerFactory) {
            this.basePollerFactory = basePollerFactory;
            return this;
        }

        @Override
        public BranchPredicateStep branchQueue(final IndexedTransactionalQueue branchQueue) {
            this.branchQueue = branchQueue;
            return this;
        }

        @Override
        public BuildStep branchPredicate(final IndexPredicate branchPredicate) {
            this.branchPredicate = branchPredicate;
            return this;
        }

        @Override
        public IndexedTransactionalQueue build() {
            return new BranchedIndexedTransactionalQueue(basePollerFactory, branchQueue, branchPredicate);
        }
    }
}
