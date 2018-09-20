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

import org.tools4j.eventsourcing.queue.DefaultEventProcessingQueue;
import org.tools4j.eventsourcing.step.DownstreamWhileDoneThenUpstreamOnceStep;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The Event Processing Queue provides an API to a composition of upstream and downstream queues and a processor step
 * that processes one event from upstream queue and appends one or more resulting events atomically to the downstream
 * queue.
 *
 * The appender() method returns an appender to the upstream queue.
 * The createPoller() method creates a poller for the downstream queue.
 */
public interface EventProcessingQueue extends IndexedQueue {
    /**
     * @return a processor step that processes one event from upstream queue and appends one or more
     *         resulting events atomically to the downstream queue
     */
    Step processorStep();

    static Builder builder() {
        return new DefaultBuilder();
    }

    interface Builder {
        DownstreamQueueBuilder upstreamQueue(IndexedQueue upstreamQueue);

        interface DownstreamQueueBuilder {
            UpstreamFactoryBuilder downstreamQueue(IndexedTransactionalQueue downstreamQueue);
        }

        interface UpstreamFactoryBuilder {
            DownstreamFactoryBuilder upstreamFactory(MessageConsumer.UpstreamFactory upstreamFactory);
        }

        interface DownstreamFactoryBuilder {
            OptionalsBuilder downstreamFactory(MessageConsumer.DownstreamFactory downstreamFactory);
        }

        interface OptionalsBuilder {
            OptionalsBuilder systemNanoClock(LongSupplier systemNanoClock);

            OptionalsBuilder leadership(BooleanSupplier leadership);

            OptionalsBuilder onUpstreamProcessingStart(Poller.IndexConsumer onUpstreamProcessingStart);

            OptionalsBuilder onUpstreamProcessingComplete(Poller.IndexConsumer onUpstreamProcessingComplete);

            OptionalsBuilder onDownstreamProcessingStart(Poller.IndexConsumer onDownstreamProcessingStart);

            OptionalsBuilder onDownstreamProcessingCompleted(Poller.IndexConsumer onDownstreamProcessingCompleted);

            OptionalsBuilder processorStepFactory(BinaryOperator<Step> processorStepFactory);

            EventProcessingQueue build() throws IOException;
        }
    }

    class DefaultBuilder implements Builder, Builder.DownstreamQueueBuilder, Builder.UpstreamFactoryBuilder, Builder.DownstreamFactoryBuilder, Builder.OptionalsBuilder {
        private IndexedQueue upstreamQueue;
        private IndexedTransactionalQueue downstreamQueue;
        private LongSupplier systemNanoClock = System::nanoTime;
        private BooleanSupplier leadership = () -> true;
        private Poller.IndexConsumer onUpstreamProcessingStart = Poller.IndexConsumer.noop();
        private Poller.IndexConsumer onUpstreamProcessingComplete = Poller.IndexConsumer.noop();
        private Poller.IndexConsumer onDownstreamProcessingStart = Poller.IndexConsumer.noop();
        private Poller.IndexConsumer onDownstreamProcessingCompleted = Poller.IndexConsumer.noop();
        private MessageConsumer.UpstreamFactory upstreamFactory = MessageConsumer.UpstreamFactory.PASS_THROUGH;
        private MessageConsumer.DownstreamFactory downstreamFactory = MessageConsumer.DownstreamFactory.NO_OP;
        private BinaryOperator<Step> processorStepFactory = DownstreamWhileDoneThenUpstreamOnceStep::new;

        @Override
        public DownstreamQueueBuilder upstreamQueue(final IndexedQueue upstreamQueue) {
            this.upstreamQueue = upstreamQueue;
            return this;
        }

        @Override
        public UpstreamFactoryBuilder downstreamQueue(final IndexedTransactionalQueue downstreamQueue) {
            this.downstreamQueue = downstreamQueue;
            return this;
        }

        @Override
        public DownstreamFactoryBuilder upstreamFactory(final MessageConsumer.UpstreamFactory upstreamFactory) {
            this.upstreamFactory = upstreamFactory;
            return this;
        }

        @Override
        public OptionalsBuilder downstreamFactory(final MessageConsumer.DownstreamFactory downstreamFactory) {
            this.downstreamFactory = downstreamFactory;
            return this;
        }

        @Override
        public OptionalsBuilder systemNanoClock(final LongSupplier systemNanoClock) {
            this.systemNanoClock = systemNanoClock;
            return this;
        }

        @Override
        public OptionalsBuilder leadership(final BooleanSupplier leadership) {
            this.leadership = leadership;
            return this;
        }

        @Override
        public OptionalsBuilder onUpstreamProcessingStart(final Poller.IndexConsumer onUpstreamProcessingStart) {
            this.onUpstreamProcessingStart = onUpstreamProcessingStart;
            return this;
        }

        @Override
        public OptionalsBuilder onUpstreamProcessingComplete(final Poller.IndexConsumer onUpstreamProcessingComplete) {
            this.onUpstreamProcessingComplete = onUpstreamProcessingComplete;
            return this;
        }

        @Override
        public OptionalsBuilder onDownstreamProcessingStart(final Poller.IndexConsumer onDownstreamProcessingStart) {
            this.onDownstreamProcessingStart = onDownstreamProcessingStart;
            return this;
        }

        @Override
        public OptionalsBuilder onDownstreamProcessingCompleted(final Poller.IndexConsumer onDownstreamProcessingCompleted) {
            this.onDownstreamProcessingCompleted = onDownstreamProcessingCompleted;
            return this;
        }

        @Override
        public OptionalsBuilder processorStepFactory(final BinaryOperator<Step> processorStepFactory) {
            this.processorStepFactory = processorStepFactory;
            return this;
        }

        @Override
        public EventProcessingQueue build() throws IOException {
            return new DefaultEventProcessingQueue(
                    upstreamQueue,
                    downstreamQueue,
                    systemNanoClock,
                    leadership,
                    onUpstreamProcessingStart,
                    onUpstreamProcessingComplete,
                    onDownstreamProcessingStart,
                    onDownstreamProcessingCompleted,
                    upstreamFactory,
                    downstreamFactory,
                    processorStepFactory
            );
        }
    }
}
