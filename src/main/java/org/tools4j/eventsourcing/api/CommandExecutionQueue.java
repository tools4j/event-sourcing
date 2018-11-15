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

import org.tools4j.eventsourcing.common.DefaultCommandExecutionQueue;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The Command Execution Queue allows to execute commands in "once-only" way. All application state-changes
 * are to applied to the application state only once. The resulting state-changing events are persisted
 * in the event queue.
 *
 * The resulting state-changing events can be polled by multiple consumers to build their own projections.
 *
 * The appender() method returns an command queue appender.
 * The createPoller() method creates a poller for the event queue.
 */
public interface CommandExecutionQueue extends IndexedQueue {
    /**
     * @return a executor step that executes one command from the command queue and appends one or more
     *         resulting events atomically to the event queue
     */
    Step executorStep();

    static Builder builder() {
        return new DefaultBuilder();
    }

    interface Builder {
        EventQueueStep commandQueue(IndexedQueue commandQueue);

        interface EventQueueStep {
            CommandExecutorFactoryStep eventQueue(IndexedTransactionalQueue eventQueue);
        }

        interface CommandExecutorFactoryStep {
            EventApplierFactoryStep commandExecutorFactory(MessageConsumer.CommandExecutorFactory commandExecutorFactory);
        }

        interface EventApplierFactoryStep {
            OptionalsStep eventApplierFactory(MessageConsumer.EventApplierFactory eventApplierFactory);
        }

        interface OptionalsStep {
            OptionalsStep systemNanoClock(LongSupplier systemNanoClock);

            OptionalsStep leadership(BooleanSupplier leadership);

            OptionalsStep onCommandExecutionStart(Poller.IndexConsumer onCommandExecutionStart);

            OptionalsStep onCommandExecutionComplete(Poller.IndexConsumer onCommandExecutionComplete);

            OptionalsStep onEventApplyingStart(Poller.IndexConsumer onEventApplyingStart);

            OptionalsStep onEventApplyingCompleted(Poller.IndexConsumer onEventApplyingCompleted);

            CommandExecutionQueue build() throws IOException;
        }
    }

    class DefaultBuilder implements Builder, Builder.EventQueueStep, Builder.CommandExecutorFactoryStep, Builder.EventApplierFactoryStep, Builder.OptionalsStep {
        private IndexedQueue commandQueue;
        private IndexedTransactionalQueue eventQueue;
        private LongSupplier systemNanoClock = System::nanoTime;
        private BooleanSupplier leadership = () -> true;
        private Poller.IndexConsumer onCommandExecutionStart = Poller.IndexConsumer.noop();
        private Poller.IndexConsumer onCommandExecutionComplete = Poller.IndexConsumer.noop();
        private Poller.IndexConsumer onEventApplyingStart = Poller.IndexConsumer.noop();
        private Poller.IndexConsumer onEventApplyingCompleted = Poller.IndexConsumer.noop();
        private MessageConsumer.CommandExecutorFactory commandExecutorFactory = MessageConsumer.CommandExecutorFactory.PASS_THROUGH;
        private MessageConsumer.EventApplierFactory eventApplierFactory = MessageConsumer.EventApplierFactory.NO_OP;

        @Override
        public EventQueueStep commandQueue(final IndexedQueue commandQueue) {
            this.commandQueue = commandQueue;
            return this;
        }

        @Override
        public CommandExecutorFactoryStep eventQueue(final IndexedTransactionalQueue eventQueue) {
            this.eventQueue = eventQueue;
            return this;
        }

        @Override
        public EventApplierFactoryStep commandExecutorFactory(final MessageConsumer.CommandExecutorFactory commandExecutorFactory) {
            this.commandExecutorFactory = commandExecutorFactory;
            return this;
        }

        @Override
        public OptionalsStep eventApplierFactory(final MessageConsumer.EventApplierFactory eventApplierFactory) {
            this.eventApplierFactory = eventApplierFactory;
            return this;
        }

        @Override
        public OptionalsStep systemNanoClock(final LongSupplier systemNanoClock) {
            this.systemNanoClock = Objects.requireNonNull(systemNanoClock);
            return this;
        }

        @Override
        public OptionalsStep leadership(final BooleanSupplier leadership) {
            this.leadership = Objects.requireNonNull(leadership);
            return this;
        }

        @Override
        public OptionalsStep onCommandExecutionStart(final Poller.IndexConsumer onCommandExecutionStart) {
            this.onCommandExecutionStart = Objects.requireNonNull(onCommandExecutionStart);
            return this;
        }

        @Override
        public OptionalsStep onCommandExecutionComplete(final Poller.IndexConsumer onCommandExecutionComplete) {
            this.onCommandExecutionComplete = Objects.requireNonNull(onCommandExecutionComplete);
            return this;
        }

        @Override
        public OptionalsStep onEventApplyingStart(final Poller.IndexConsumer onEventApplyingStart) {
            this.onEventApplyingStart = Objects.requireNonNull(onEventApplyingStart);
            return this;
        }

        @Override
        public OptionalsStep onEventApplyingCompleted(final Poller.IndexConsumer onEventApplyingCompleted) {
            this.onEventApplyingCompleted = Objects.requireNonNull(onEventApplyingCompleted);
            return this;
        }

        @Override
        public CommandExecutionQueue build() throws IOException {
            return new DefaultCommandExecutionQueue(
                    commandQueue,
                    eventQueue,
                    systemNanoClock,
                    leadership,
                    onCommandExecutionStart,
                    onCommandExecutionComplete,
                    onEventApplyingStart,
                    onEventApplyingCompleted,
                    commandExecutorFactory,
                    eventApplierFactory
            );
        }
    }
}
