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
package org.tools4j.eventsourcing.application;

import org.agrona.DirectBuffer;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Header;

public interface Queue {
    Appender appender();
    Poller poller();
    long size();

    interface Appender {
        void enqueue(short templateId, int inputSourceId, long sourceSeqNo,
                     long eventTimeNanosSinceEpoch, int userData,
                     DirectBuffer message, int offset, int length);
        void enqueue(Header header, DirectBuffer message, int offset, int length);
        default void enqueue(final Event event) {
            enqueue(event.header(), event.payload(), 0, event.payloadLength());
        }
    }

    enum PollResut {
        POLLED, ADMIN, END;
    }
    interface Poller {
        PollResut poll(EventConsuer consumer);
    }

    @FunctionalInterface
    interface EventConsuer {
        void consume(int queueIndex, Event event);
    }
}
