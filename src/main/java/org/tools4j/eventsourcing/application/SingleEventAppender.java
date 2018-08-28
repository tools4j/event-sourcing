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
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Header;

import java.util.Objects;

public class SingleEventAppender implements Queue.Appender {

    private static final DirectBuffer EMPTY_PAYLOAD = new UnsafeBuffer(0, 0);

    private final ApplicationHandler applicationHandler;
    private final Queue.Appender outputAppender;
    private final Queue.EventConsuer inputEventHandler;

    private int enqueued;

    public SingleEventAppender(final ApplicationHandler applicationHandler,
                               final Queue.Appender outputAppender) {
        this.applicationHandler = Objects.requireNonNull(applicationHandler);
        this.outputAppender = Objects.requireNonNull(outputAppender);
        this.inputEventHandler = this::hxandleInputEvent;
    }

    private void hxandleInputEvent(final int queueIndex, final Event event) {
        enqueued = 0;
        applicationHandler.processInputEvent(queueIndex, event, this);
        if (enqueued == 0) {
            enqueueEmpty(queueIndex, event);
        } else {
            enqueued = 0;
        }
    }

    private void enqueueEmpty(final int inputQueueIndex, final Event event) {
        final Header header = event.header();
        outputAppender.enqueue(Header.ADMIN_TEMPLATE_ID, header.inputSourceId(), header.sourceSeqNo(),
                header.eventTimeNanosSinceEpoch(), 0, EMPTY_PAYLOAD, 0, 0);
    }

    private void checkState(final int inputSourceid, final long sourceSeqNo) {
        if (enqueued > 0) {
            throw new IllegalStateException("At most one event can be appended to the output queue per input event; " +
                    "an output event has already been appended for inputSourceId=" +
                    inputSourceid + " and sourceSeqNo=" + sourceSeqNo);
        }
    }

    public Queue.EventConsuer inputEventHandler() {
        return inputEventHandler;
    }

    @Override
    public void enqueue(final Event event) {
        checkState(event.header().inputSourceId(), event.header().sourceSeqNo());
        outputAppender.enqueue(event);
        enqueued++;
    }

    @Override
    public void enqueue(final Header header, final DirectBuffer message, final int offset, final int length) {
        checkState(header.inputSourceId(), header.sourceSeqNo());
        outputAppender.enqueue(header, message, offset, length);
        enqueued++;
    }

    @Override
    public void enqueue(final short templateId, final int inputSourceId, final long sourceSeqNo,
                        final long eventTimeNanosSinceEpoch, final int userData,
                        final DirectBuffer message, final int offset, final int length) {
        checkState(inputSourceId, sourceSeqNo);
        outputAppender.enqueue(templateId, inputSourceId, sourceSeqNo, eventTimeNanosSinceEpoch, userData, message, offset, length);
        enqueued++;
    }
}
