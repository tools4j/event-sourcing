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

import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.api.*;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public final class ReplicatedExecutionQueue implements ExecutionQueue {

    private final IndexedQueue commandQueue;
    private final ExecutionQueue eventQueue;
    private final Step executorStep;

    private final Poller commandExecutionPoller;
    private final Poller committedEventApplyingPoller;

    public ReplicatedExecutionQueue(final IndexedQueue commandQueue,
                                    final EventQueueFactory eventQueueFactory,
                                    final LongSupplier systemNanoClock,
                                    final IndexConsumer onStartCommandExecutionHandler,
                                    final IndexConsumer onCompleteCommandExecutionHandler,
                                    final IndexConsumer onStartEventApplyingHandler,
                                    final IndexConsumer onCompletedEventApplyingHandler,
                                    final CommandExecutorFactory commandExecutorFactory,
                                    final EventApplierFactory eventApplierFactory,
                                    final Runnable onStateReset,
                                    final int encodingBufferSize) throws IOException {
        this.commandQueue = Objects.requireNonNull(commandQueue);

        final DefaultProgressState currentProgressState = new DefaultProgressState(systemNanoClock);
        final DefaultProgressState completedProgressState = new DefaultProgressState(systemNanoClock);


        this.eventQueue = eventQueueFactory.create(() -> {
            currentProgressState.reset();
            completedProgressState.reset();
            onStateReset.run();
        });

        final BooleanSupplier leadership = eventQueue::leader;

        final Transaction eventAppender = new MultiPayloadAppender(eventQueue.appender(), new UnsafeBuffer(ByteBuffer.allocateDirect(encodingBufferSize)));

        final MessageConsumer uncommittedEventApplier = eventApplierFactory.create(
                currentProgressState,
                completedProgressState);

        final MessageConsumer appenderAndApplierOfUncommittedEvents =
                eventAppender.andThen(uncommittedEventApplier);

        this.commandExecutionPoller = commandQueue.createPoller(
                Poller.Options.builder()
                        .skipWhen(
                            IndexPredicate.isLeader(leadership)
                            .and(
                                    (index, source, sourceSeq, eventTimeNanos) -> sourceSeq <= eventQueue.appender().lastSourceSeq(source)
                            )
                            .or(
                                    IndexPredicate.isNotLeader(leadership)
                                    .and(
                                            IndexPredicate.isNotAheadOf(completedProgressState))
                                    )
                            )
                        .resetWhen(currentProgressState::commandPollerResetRequired)
                        .pauseWhen(
                            IndexPredicate.isNotLeader(leadership)
                        )
                        .onProcessingStart(
                                currentProgressState
                                        .andThen(IndexConsumer.transactionInit(eventAppender))
                                        .andThen(onStartCommandExecutionHandler))
                        .onProcessingComplete(
                                IndexConsumer.transactionCommitAndPushNoops(eventAppender)
                                        .andThen(completedProgressState)
                                        .andThen(onCompleteCommandExecutionHandler))
                        .onReset(currentProgressState::resetCommandPoller)
                        .build()
        );

        final MessageConsumer commandExecutor = commandExecutorFactory.create(
                appenderAndApplierOfUncommittedEvents,
                currentProgressState,
                completedProgressState);

        final Step commandExecutionStep = new PollingProcessStep(this.commandExecutionPoller, commandExecutor);

        final MessageConsumer committedEventApplier = eventApplierFactory.create(
                currentProgressState,
                completedProgressState);

        this.committedEventApplyingPoller = eventQueue.createPoller(
                Poller.Options.builder()
                        .skipWhen(
                                IndexPredicate.isNotAheadOf(completedProgressState))
                        .resetWhen(currentProgressState::eventPollerResetRequired)
                        .onProcessingStart(
                                currentProgressState.andThen(onStartEventApplyingHandler))
                        .onProcessingComplete(
                                completedProgressState.andThen(onCompletedEventApplyingHandler))
                        .onProcessingSkipped(
                                // skip is equivalent to committed as we apply changes to state in command executor and skip when
                                // event matches command source/sourceSeq
                                currentProgressState.andThen(completedProgressState))
                        .onReset(currentProgressState::resetEventPoller)
                        .bufferPoller(new PayloadBufferPoller())
                        .build()
        );

        final Step committedEventApplyingStep = new PollingProcessStep(this.committedEventApplyingPoller, committedEventApplier);

        final Step step =  new ApplyAllExecuteOnceStep(commandExecutionStep, committedEventApplyingStep);


        this.executorStep = () -> step.perform() | eventQueue.executorStep().perform();
    }

    @Override
    public IndexedAppender appender() {
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
    public boolean leader() {
        return eventQueue.leader();
    }

    @Override
    public void init() {
        eventQueue.init();
    }

    @Override
    public void close() {
        commandQueue.close();
        eventQueue.close();
        commandExecutionPoller.close();
        committedEventApplyingPoller.close();
    }
}
