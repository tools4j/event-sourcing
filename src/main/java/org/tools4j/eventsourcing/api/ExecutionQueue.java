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

import org.tools4j.eventsourcing.common.ReplicatedExecutionQueue;
import org.tools4j.eventsourcing.common.StandaloneExecutionQueue;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * The Execution Queue allows to execute commands in "once-only" way. All application state-changes
 * are to applied to the application state only once. The resulting state-changing events are persisted
 * in the event queue.
 *
 * The resulting state-changing events can be polled by multiple consumers to build their own projections.
 *
 * The appender() method returns an command queue appender.
 * The createPoller() method creates a poller for the event queue.
 */
public interface ExecutionQueue extends IndexedQueue {
    /**
     * @return a executor step that executes one command from the command queue and appends one or more
     *         resulting events atomically to the event queue
     */
    Step executorStep();

    void init();

    boolean leader();

    static Builder builder() {
        return new DefaultBuilder();
    }

    interface EventQueueFactory {
        ExecutionQueue create(Runnable onStateReset) throws IOException;
    }

    interface Builder {
        EventQueueFactoryStep commandQueue(IndexedQueue commandQueue);

        interface EventQueueFactoryStep {
            CommandExecutorFactoryStep eventQueue(IndexedQueue eventQueue);
            CommandExecutorFactoryStep eventQueueFactory(EventQueueFactory eventQueueFactory);
        }

        interface CommandExecutorFactoryStep {
            EventApplierFactoryStep commandExecutorFactory(CommandExecutorFactory commandExecutorFactory);
        }

        interface EventApplierFactoryStep {
            OptionalsStep eventApplierFactory(EventApplierFactory eventApplierFactory);
        }

        interface OptionalsStep {
            OptionalsStep transactionBufferSize(int transactionBufferSize);

            OptionalsStep systemNanoClock(LongSupplier systemNanoClock);

            OptionalsStep onCommandExecutionStart(IndexConsumer onCommandExecutionStart);

            OptionalsStep onCommandExecutionComplete(IndexConsumer onCommandExecutionComplete);

            OptionalsStep onEventApplyingStart(IndexConsumer onEventApplyingStart);

            OptionalsStep onEventApplyingCompleted(IndexConsumer onEventApplyingCompleted);

            OptionalsStep onStateReset(Runnable onStateReset);

            ExecutionQueue build() throws IOException;
        }
    }

    class DefaultBuilder implements Builder, Builder.EventQueueFactoryStep, Builder.CommandExecutorFactoryStep, Builder.EventApplierFactoryStep, Builder.OptionalsStep {
        private IndexedQueue commandQueue;
        private IndexedQueue eventQueue;
        private EventQueueFactory eventQueueFactory;
        private LongSupplier systemNanoClock = System::nanoTime;
        private IndexConsumer onCommandExecutionStart = IndexConsumer.noop();
        private IndexConsumer onCommandExecutionComplete = IndexConsumer.noop();
        private IndexConsumer onEventApplyingStart = IndexConsumer.noop();
        private IndexConsumer onEventApplyingCompleted = IndexConsumer.noop();
        private CommandExecutorFactory commandExecutorFactory = CommandExecutorFactory.PASS_THROUGH;
        private EventApplierFactory eventApplierFactory = EventApplierFactory.NO_OP;
        private int transationBufferSize = 16 * 1024;
        private Runnable onStateReset = () -> {};

        @Override
        public EventQueueFactoryStep commandQueue(final IndexedQueue commandQueue) {
            this.commandQueue = commandQueue;
            return this;
        }

        @Override
        public CommandExecutorFactoryStep eventQueue(final IndexedQueue eventQueue) {
            this.eventQueue = eventQueue;
            return this;
        }

        @Override
        public CommandExecutorFactoryStep eventQueueFactory(final EventQueueFactory eventQueueFactory) {
            this.eventQueueFactory = eventQueueFactory;
            return this;
        }

        @Override
        public EventApplierFactoryStep commandExecutorFactory(final CommandExecutorFactory commandExecutorFactory) {
            this.commandExecutorFactory = commandExecutorFactory;
            return this;
        }

        @Override
        public OptionalsStep eventApplierFactory(final EventApplierFactory eventApplierFactory) {
            this.eventApplierFactory = eventApplierFactory;
            return this;
        }

        @Override
        public OptionalsStep transactionBufferSize(final int transactionBufferSize) {
            this.transationBufferSize = transactionBufferSize;
            return this;
        }

        @Override
        public OptionalsStep systemNanoClock(final LongSupplier systemNanoClock) {
            this.systemNanoClock = Objects.requireNonNull(systemNanoClock);
            return this;
        }

        @Override
        public OptionalsStep onCommandExecutionStart(final IndexConsumer onCommandExecutionStart) {
            this.onCommandExecutionStart = Objects.requireNonNull(onCommandExecutionStart);
            return this;
        }

        @Override
        public OptionalsStep onCommandExecutionComplete(final IndexConsumer onCommandExecutionComplete) {
            this.onCommandExecutionComplete = Objects.requireNonNull(onCommandExecutionComplete);
            return this;
        }

        @Override
        public OptionalsStep onEventApplyingStart(final IndexConsumer onEventApplyingStart) {
            this.onEventApplyingStart = Objects.requireNonNull(onEventApplyingStart);
            return this;
        }

        @Override
        public OptionalsStep onEventApplyingCompleted(final IndexConsumer onEventApplyingCompleted) {
            this.onEventApplyingCompleted = Objects.requireNonNull(onEventApplyingCompleted);
            return this;
        }

        @Override
        public OptionalsStep onStateReset(final Runnable onStateReset) {
            this.onStateReset = onStateReset;
            return this;
        }

        @Override
        public ExecutionQueue build() throws IOException {
            if (eventQueue != null) {
                return new StandaloneExecutionQueue(
                        commandQueue,
                        eventQueue,
                        systemNanoClock,
                        onCommandExecutionStart,
                        onCommandExecutionComplete,
                        onEventApplyingStart,
                        onEventApplyingCompleted,
                        commandExecutorFactory,
                        eventApplierFactory,
                        transationBufferSize
                );
            } else {
                return new ReplicatedExecutionQueue(
                        commandQueue,
                        eventQueueFactory,
                        systemNanoClock,
                        onCommandExecutionStart,
                        onCommandExecutionComplete,
                        onEventApplyingStart,
                        onEventApplyingCompleted,
                        commandExecutorFactory,
                        eventApplierFactory,
                        onStateReset,
                        transationBufferSize);
            }
        }
    }
}
