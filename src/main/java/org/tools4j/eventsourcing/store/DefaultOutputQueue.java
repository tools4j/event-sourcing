/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eventsourcing.store;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.DefaultEvent;

import java.util.Objects;

public class DefaultOutputQueue implements OutputQueue {

    private final Store store;

    public DefaultOutputQueue(final Store store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public Appender appender() {
        return new OutputQueueAppender();
    }

    @Override
    public Poller poller() {
        return new OutputQueuePoller();
    }

    @Override
    public long size() {
        return store.size();
    }

    private final class OutputQueueAppender implements Appender {

        final Store.Appender storeAppender = store.appender();
        final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();

        @Override
        public long append(final Event event) {
            final int length = encode(event);
            return storeAppender.append(buffer, 0, length);
        }

        @Override
        public boolean compareAndAppend(final long expectedIndex, final Event event) {
            if (expectedIndex == store.size()) {//TODO there could be racing with multiple threads, concern?
                final int length = encode(event);
                storeAppender.append(buffer, 0, length);
                return storeAppender.compareAndAppend(expectedIndex, buffer, 0, length);
            }
            return false;
        }

        private int encode(final Event event) {
            final int payloadLength = event.payloadLength();
            final int headerLength = event.header().writeTo(buffer, 0);
            event.payload().getBytes(0, buffer, headerLength, payloadLength);
            return headerLength + payloadLength;
        }
    }

    private final class OutputQueuePoller implements Poller {

        final Store.Poller storePoller = store.poller();
        final DefaultEvent event = new DefaultEvent();
        final Store.EventConsumer eventConsumer = this::consume;
        long storeIndex;

        @Override
        public Poller nextIndex(final long index) {
            storePoller.nextIndex(index);
            return this;
        }

        @Override
        public boolean poll(final LongObjConsumer<? super Event> consumer) {
            if (storePoller.poll(eventConsumer)) {
                try {
                    consumer.accept(storeIndex, event);
                    return true;
                } finally {
                    storeIndex = -1;
                    event.unwrap();
                }
            }
            return false;
        }

        private void consume(final long storeIndex, final DirectBuffer event, final int offset, final int length) {
            this.storeIndex = storeIndex;
            this.event.wrap(event, offset, length);
        }
    }
}
