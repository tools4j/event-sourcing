/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eso.evt;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import static org.tools4j.eso.evt.FlyweightEvent.PAYLOAD_OFFSET;

public enum AdminEvents {
    ;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer(0, 0);

    private static final int TIMER_TYPE_OFFSET = 0;
    private static final int TIMER_TYPE_LENGTH = Integer.BYTES;
    private static final int TIMER_ID_OFFSET = TIMER_TYPE_OFFSET + TIMER_TYPE_LENGTH;
    private static final int TIMER_ID_LENGTH = Long.BYTES;
    private static final int TIMER_TIMEOUT_OFFSET = TIMER_ID_OFFSET + TIMER_ID_LENGTH;
    private static final int TIMER_TIMEOUT_LENGTH = Long.BYTES;
    private static final int TIMER_PAYLOAD_SIZE = TIMER_TYPE_LENGTH + TIMER_ID_LENGTH +
            TIMER_TIMEOUT_LENGTH;


    public static FlyweightEvent noop(final FlyweightEvent event,
                                      final MutableDirectBuffer buffer,
                                      final int offset,
                                      final int source,
                                      final long sequence,
                                      final int index,
                                      final long time) {
        return event.init(buffer, offset, source, sequence, index, EventType.NOOP.value(), time,
                EMPTY_BUFFER, 0, 0);
    }

    public static FlyweightEvent timerStarted(final FlyweightEvent event,
                                              final MutableDirectBuffer buffer,
                                              final int offset,
                                              final int source,
                                              final long sequence,
                                              final int index,
                                              final long time,
                                              final int timerType,
                                              final long timerId,
                                              final long timeout) {
        return timerEvent(event, buffer, offset, source, sequence, index,
                EventType.TIMER_STARTED, time, timerType, timerId, timeout);
    }

    public static FlyweightEvent timerStopped(final FlyweightEvent event,
                                              final MutableDirectBuffer buffer,
                                              final int offset,
                                              final int source,
                                              final long sequence,
                                              final int index,
                                              final long time,
                                              final int timerType,
                                              final long timerId,
                                              final long timeout) {
        return timerEvent(event, buffer, offset, source, sequence, index,
                EventType.TIMER_STOPPED, time, timerType, timerId, timeout);
    }

    public static FlyweightEvent timerExpired(final FlyweightEvent event,
                                              final MutableDirectBuffer buffer,
                                              final int offset,
                                              final int source,
                                              final long sequence,
                                              final int index,
                                              final long time,
                                              final int timerType,
                                              final long timerId,
                                              final long timeout) {
        return timerEvent(event, buffer, offset, source, sequence, index,
                EventType.TIMER_EXPIRED, time, timerType, timerId, timeout);
    }

    private static FlyweightEvent timerEvent(final FlyweightEvent event,
                                             final MutableDirectBuffer buffer,
                                             final int offset,
                                             final int source,
                                             final long sequence,
                                             final int index,
                                             final EventType type,
                                             final long time,
                                             final int timerType,
                                             final long timerId,
                                             final long timeout) {
        FlyweightEvent.writeHeaderTo(buffer, offset, source, sequence, index, type.value(), time, TIMER_PAYLOAD_SIZE);
        buffer.putInt(offset + PAYLOAD_OFFSET + TIMER_TYPE_OFFSET, timerType);
        buffer.putLong(offset + PAYLOAD_OFFSET + TIMER_ID_OFFSET, timerId);
        buffer.putLong(offset + PAYLOAD_OFFSET + TIMER_TIMEOUT_OFFSET, timeout);
        return event.init(buffer, offset);
    }
}
