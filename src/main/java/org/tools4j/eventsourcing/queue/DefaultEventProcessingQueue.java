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
package org.tools4j.eventsourcing.queue;

import org.tools4j.eventsourcing.api.*;
import org.tools4j.eventsourcing.poller.DefaultEventProcessingState;
import org.tools4j.eventsourcing.step.PollingProcessStep;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public final class DefaultEventProcessingQueue implements EventProcessingQueue {
    private final IndexedQueue upstreamQueue;
    private final IndexedTransactionalQueue downstreamQueue;
    private final Step processorStep;

    private final Poller upstreamProcessorPoller;
    private final Poller downstreamProcessorPoller;

    public DefaultEventProcessingQueue(final IndexedQueue upstreamQueue,
                                       final IndexedTransactionalQueue downstreamQueue,
                                       final LongSupplier systemNanoClock,
                                       final BooleanSupplier leadership,
                                       final Poller.IndexConsumer onStartUpstreamProcessingHandler,
                                       final Poller.IndexConsumer onCompleteUpstreamProcessingHandler,
                                       final Poller.IndexConsumer onStartDownstreamProcessingHandler,
                                       final Poller.IndexConsumer onCompletedDownstreamProcessingHandler,
                                       final MessageConsumer.UpstreamFactory upstreamFactory,
                                       final MessageConsumer.DownstreamFactory downstreamFactory,
                                       final BinaryOperator<Step> processorStepFactory) throws IOException {
        this.upstreamQueue = Objects.requireNonNull(upstreamQueue);
        this.downstreamQueue = Objects.requireNonNull(downstreamQueue);

        final DefaultEventProcessingState currentUpstreamState = new DefaultEventProcessingState(systemNanoClock);
        final DefaultEventProcessingState completedUpstreamState = new DefaultEventProcessingState(systemNanoClock);
        final DefaultEventProcessingState currentDownstreamState = new DefaultEventProcessingState(systemNanoClock);
        final DefaultEventProcessingState completedDownstreamState = new DefaultEventProcessingState(systemNanoClock);

        final Transaction downstreamAppender = downstreamQueue.appender();

        this.upstreamProcessorPoller = upstreamQueue.createPoller(
                Poller.Options.builder()
                        .skipWhen(
                            Poller.IndexPredicate.isLessThanOrEqual(completedDownstreamState))
                        .pauseWhen(
                            Poller.IndexPredicate.isNotLeader(leadership)
                                .and(Poller.IndexPredicate.isGreaterThan(completedDownstreamState))
                                .or(Poller.IndexPredicate.isTrue(() ->
                                        completedUpstreamState.isAheadOf(completedDownstreamState))))
                        .onProcessingStart(
                                currentUpstreamState
                                        .andThen(Poller.IndexConsumer.transactionInit(downstreamAppender))
                                        .andThen(onStartUpstreamProcessingHandler))
                        .onProcessingComplete(
                                Poller.IndexConsumer.transactionCommit(downstreamAppender)
                                        .andThen(completedUpstreamState)
                                        .andThen(onCompleteUpstreamProcessingHandler)
                        ).build()
        );

        final MessageConsumer upstreamMessageConsumer = upstreamFactory.create(
                downstreamAppender,
                currentUpstreamState,
                completedDownstreamState);

        final MessageConsumer downstreamMessageConsumer = downstreamFactory.create(
                currentDownstreamState,
                completedDownstreamState);

        final Step upstreamProcessorStep = new PollingProcessStep(this.upstreamProcessorPoller, upstreamMessageConsumer);

        this.downstreamProcessorPoller = downstreamQueue.createPoller(
                Poller.Options.builder().skipWhen(
                                Poller.IndexPredicate.isEqualTo(completedUpstreamState))
                        .onProcessingStart(
                                currentDownstreamState.andThen(onStartDownstreamProcessingHandler))
                        .onProcessingComplete(
                                completedDownstreamState.andThen(onCompletedDownstreamProcessingHandler))
                        .onProcessingSkipped(
                                // skip is equivalent to committed as we apply changes to state in upstream processor and skip when
                                // downstream matches upstream source/sourceId
                                currentDownstreamState.andThen(completedDownstreamState))
                        .build()
        );

        final Step downstreamProcessorStep = new PollingProcessStep(this.downstreamProcessorPoller, downstreamMessageConsumer);

        this.processorStep = processorStepFactory.apply(upstreamProcessorStep, downstreamProcessorStep);
    }

    @Override
    public IndexedMessageConsumer appender() {
        return upstreamQueue.appender();
    }

    @Override
    public Step processorStep() {
        return processorStep;
    }

    @Override
    public Poller createPoller(final Poller.Options options) throws IOException {
        return downstreamQueue.createPoller(options);
    }

    @Override
    public void close() {
        upstreamQueue.close();
        downstreamQueue.close();
        upstreamProcessorPoller.close();
        downstreamProcessorPoller.close();
    }
}
