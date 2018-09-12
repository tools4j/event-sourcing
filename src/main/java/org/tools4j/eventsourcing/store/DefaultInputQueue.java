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
import org.tools4j.eventsourcing.event.DefaultEvent;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.DataHeader;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class DefaultInputQueue implements InputQueue {

    private final Store store;

    public DefaultInputQueue(final Store store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public Appender appender() {
        return new InputQueueAppender();
    }

    @Override
    public Poller poller() {
        return new DefaultInputPoller();
    }

    @Override
    public long size() {
        return store.size();
    }

    private final class InputQueueAppender implements Appender {

        final Store.Appender storeAppender = store.appender();
        final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        final DataHeader.Mutable header = DataHeader.allocateDirect();

        @Override
        public void append(final int sourceId, final long sourceSeqNo, final short subtypeId, final int userData,
                           final long eventTimeNanosSinceEpoch,
                           final DirectBuffer payload, final int offset, final int length) {
            final int headerLength = header.version(Version.current())
                    .subtypeId(subtypeId)
                    .inputSourceId(sourceId)
                    .sourceSeqNo(sourceSeqNo)
                    .eventTimeNanosSinceEpoch(eventTimeNanosSinceEpoch)
                    .userData(userData)
                    .writeTo(buffer, 0);
            buffer.putBytes(headerLength, payload, offset, length);
            storeAppender.append(buffer, 0, headerLength + length);
        }
    }

    private final class DefaultInputPoller implements Poller {

        final Store.Poller storePoller = store.poller();
        final DefaultEvent event = new DefaultEvent();
        final Store.EventConsumer eventConsumer = this::consume;
        long lastStoreIndex = -1;

        @Override
        public Poller nextIndex(final long index) {
            storePoller.nextIndex(index);
            return this;
        }

        @Override
        public boolean poll(final Consumer<? super Event> consumer) {
            if (storePoller.poll(eventConsumer)) {
                try {
                    consumer.accept(event);
                    return true;
                } finally {
                    lastStoreIndex = -1;
                    event.unwrap();
                }
            }
            return false;
        }

        @Override
        public boolean poll(final Predicate<? super Header> condition, final Consumer<? super Event> consumer) {
            if (storePoller.poll(eventConsumer)) {
                try {
                    if (condition.test(event.header())) {
                        consumer.accept(event);
                        return true;
                    } else {
                        storePoller.nextIndex(lastStoreIndex);
                        return false;
                    }
                } finally {
                    lastStoreIndex = -1;
                    event.unwrap();
                }
            }
            return false;
        }

        private void consume(final long storeIndex, final DirectBuffer event, final int offset, final int length) {
            lastStoreIndex = storeIndex;
            event.wrap(event, offset, length);
        }
    }
}
