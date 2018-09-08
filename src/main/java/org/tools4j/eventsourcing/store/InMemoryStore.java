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
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

public class InMemoryStore implements Store {

    private final List<DirectBuffer> events = new ArrayList<>();
    private final Appender appender = new InMemoryAppender();

    @Override
    public Appender appender() {
        return appender;
    }

    @Override
    public Poller poller() {
        return new InMemoryPoller();
    }

    @Override
    public long size() {
        synchronized (events) {
            return events.size();
        }
    }

    private final class InMemoryAppender implements Appender {
        @Override
        public long append(final DirectBuffer event, final int offset, final int length) {
            final long index;
            final DirectBuffer copy = copy(event, offset, length);
            synchronized (events) {
                index = events.size();
                events.add(copy);
            }
            return index;
        }

        @Override
        public boolean compareAndAppend(long expectedIndex, DirectBuffer event, int offset, int length) {
            final DirectBuffer copy = copy(event, offset, length);
            synchronized (events) {
                if (expectedIndex == events.size()) {
                    events.add(copy);
                    return true;
                }
            }
            return false;
        }
    }

    private final class InMemoryPoller implements Poller {
        int index = -1;

        @Override
        public Poller nextIndex(final long index) {
            if (index < 0) {
                throw new IndexOutOfBoundsException("Index cannot be negative: " + index);
            }
            synchronized (this) {
                this.index = (int)Math.min(index, Integer.MAX_VALUE);
            }
            return this;
        }

        @Override
        public boolean poll(final EventConsumer consumer) {
            synchronized (this) {
                synchronized (events) {
                    final int next = index + 1;
                    if (next >= 0 & next < events.size()) {//index could be negative due to overflow
                        final DirectBuffer result = events.get(next);
                        consumer.consume(next, copy(result, 0, result.capacity()), 0, result.capacity());
                        index = next;
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static DirectBuffer copy(final DirectBuffer event, final int offset, final int length) {
        final byte[] data = new byte[length];
        event.getBytes(offset, data, 0, length);
        return new UnsafeBuffer(data);
    }
}
