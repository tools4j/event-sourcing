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

import org.tools4j.eventsourcing.api.CommandExecutionQueue;
import org.tools4j.eventsourcing.api.IndexedMessageConsumer;
import org.tools4j.eventsourcing.api.IndexedQueue;
import org.tools4j.eventsourcing.api.IndexedTransactionalQueue;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.api.Transaction;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public final class DefaultCommandExecutionQueue implements CommandExecutionQueue {
    private final IndexedQueue commandQueue;
    private final IndexedTransactionalQueue eventQueue;
    private final Step executorStep;

    private final Poller commandExecutionPoller;
    private final Poller eventApplyingPoller;

    public DefaultCommandExecutionQueue(final IndexedQueue commandQueue,
                                        final IndexedTransactionalQueue eventQueue,
                                        final LongSupplier systemNanoClock,
                                        final BooleanSupplier leadership,
                                        final Poller.IndexConsumer onStartCommandExecutionHandler,
                                        final Poller.IndexConsumer onCompleteCommandExecutionHandler,
                                        final Poller.IndexConsumer onStartEventApplyingHandler,
                                        final Poller.IndexConsumer onCompletedEventApplyingHandler,
                                        final MessageConsumer.CommandExecutorFactory commandExecutorFactory,
                                        final MessageConsumer.EventApplierFactory eventApplierFactory,
                                        final BinaryOperator<Step> executorStepFactory) throws IOException {
        this.commandQueue = Objects.requireNonNull(commandQueue);
        this.eventQueue = Objects.requireNonNull(eventQueue);

        final DefaultProgressState currentCommandExecutionState = new DefaultProgressState(systemNanoClock);
        final DefaultProgressState completedCommandExecutionState = new DefaultProgressState(systemNanoClock);
        final DefaultProgressState currentEventApplyingState = new DefaultProgressState(systemNanoClock);
        final DefaultProgressState completedEventApplyingState = new DefaultProgressState(systemNanoClock);

        final Transaction eventAppender = eventQueue.appender();

        this.commandExecutionPoller = commandQueue.createPoller(
                Poller.Options.builder()
                        .skipWhen(
                            Poller.IndexPredicate.isLessThanOrEqual(completedEventApplyingState))
                        .pauseWhen(
                            Poller.IndexPredicate.isNotLeader(leadership)
                                .and(Poller.IndexPredicate.isGreaterThan(completedEventApplyingState))
                                .or(Poller.IndexPredicate.isTrue(() ->
                                        completedCommandExecutionState.isAheadOf(completedEventApplyingState))))
                        .onProcessingStart(
                                currentCommandExecutionState
                                        .andThen(Poller.IndexConsumer.transactionInit(eventAppender))
                                        .andThen(onStartCommandExecutionHandler))
                        .onProcessingComplete(
                                Poller.IndexConsumer.transactionCommitAndPushNoops(eventAppender, completedCommandExecutionState, completedEventApplyingState)
                                        .andThen(completedCommandExecutionState)
                                        .andThen(onCompleteCommandExecutionHandler))
                        .build()
        );

        final MessageConsumer commandExecutor = commandExecutorFactory.create(
                eventAppender,
                currentCommandExecutionState,
                completedEventApplyingState);

        final MessageConsumer eventApplier = eventApplierFactory.create(
                currentEventApplyingState,
                completedEventApplyingState);

        final Step commandExecutionStep = new PollingProcessStep(this.commandExecutionPoller, commandExecutor);

        this.eventApplyingPoller = eventQueue.createPoller(
                Poller.Options.builder().skipWhen(
                                Poller.IndexPredicate.isEqualTo(completedCommandExecutionState))
                        .onProcessingStart(
                                currentEventApplyingState.andThen(onStartEventApplyingHandler))
                        .onProcessingComplete(
                                completedEventApplyingState.andThen(onCompletedEventApplyingHandler))
                        .onProcessingSkipped(
                                // skip is equivalent to committed as we apply changes to state in upstream processor and skip when
                                // downstream matches upstream source/sourceSeq
                                currentEventApplyingState.andThen(completedEventApplyingState))
                        .build()
        );

        final Step eventApplyingStep = new PollingProcessStep(this.eventApplyingPoller, eventApplier);

        this.executorStep = executorStepFactory.apply(commandExecutionStep, eventApplyingStep);
    }

    @Override
    public IndexedMessageConsumer appender() {
        return commandQueue.appender();
    }

    @Override
    public Step executorStep() {
        return executorStep;
    }

    @Override
    public Poller createPoller(final Poller.Options options) throws IOException {
        return eventQueue.createPoller(options);
    }

    @Override
    public void close() {
        commandQueue.close();
        eventQueue.close();
        commandExecutionPoller.close();
        eventApplyingPoller.close();
    }
}
