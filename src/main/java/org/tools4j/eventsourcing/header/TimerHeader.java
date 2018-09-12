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
package org.tools4j.eventsourcing.header;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;

import java.util.concurrent.TimeUnit;

/**
 * Byte layout for a timeout header:
 * <pre>
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    Version    |      Type     |S|E|         Timer ID          |  0- 3
     +---------------+---------------+-------------------------------+
     |                       Input Source ID                         |  4- 7
     +-------------------------------+-------------------------------+
     |0|                   Source Sequence Number                    |  8-15
     |                      (0 for admin events)                     |
     +-------------------------------+-------------------------------+
     |                          Event Time                           | 16-23
     |                   (nanoseconds since epoch)                   |
     +-------------------------------+-------------------------------+
     |U|                   Timeout Millis/Micros                     | 24-27
     +---------------+---------------+-------------------------------+
     |                               0                               | 28-31
     +---------------+---------------+-------------------------------+

     S: Stop Flag           Started: -
     E: Expired Flag        Stopped: S
                            Expired: S+E

     U: Microseconds Flag   if set then the timeout is in microseconds
                            otherwise in milliseconds
   </pre>
 *
 * @see Header
 */
public interface TimerHeader extends Header {

    int SUBTYPE_FLAG_TIMER_STOPPED = 0x8000;
    int SUBTYPE_FLAG_TIMER_EXPIRED = 0x4000;
    int SUBTYPE_MASK_TIMER_ID      = 0x3fff;
    short TIMER_ID_MIN = 0;
    short TIMER_ID_MAX = SUBTYPE_MASK_TIMER_ID - 1;
    int TIMEOUT_FLAG_MICRO = 0x80000000;
    int TIMEOUT_MIN = 0;
    int TIMEOUT_MAX = Integer.MAX_VALUE;

    default short timerId() {
        return (short) (subtypeId() & SUBTYPE_MASK_TIMER_ID);
    }

    default boolean isStarted() {
        return (subtypeId() & SUBTYPE_FLAG_TIMER_STOPPED) == 0;
    }

    default boolean isStopped() {
        final short subtypeId = subtypeId();
        return (subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) != 0 & (subtypeId & SUBTYPE_FLAG_TIMER_EXPIRED) == 0;
    }

    default boolean isExpired() {
        final short subtypeId = subtypeId();
        return (subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) != 0 & (subtypeId & SUBTYPE_FLAG_TIMER_EXPIRED) != 0;
    }

    default boolean isMicros() {
        return (userData() & TIMEOUT_FLAG_MICRO) != 0;
    }

    default boolean isMillis() {
        return (userData() & TIMEOUT_FLAG_MICRO) == 0;
    }

    default int timeoutValue() {
        return userData() & (~TIMEOUT_FLAG_MICRO);
    }

    default long timeoutMicros() {
        final long timeout = userData();
        if ((timeout & TIMEOUT_FLAG_MICRO) != 0) {
            return timeout & (~TIMEOUT_FLAG_MICRO);
        }
        return TimeUnit.MILLISECONDS.toMicros(timeout);
    }

    class Default extends DefaultHeader implements TimerHeader {
        @Override
        public Default wrap(final Header header) {
            super.wrap(header);
            return this;
        }

        @Override
        public Default wrap(final DirectBuffer source, final int offset) {
            super.wrap(source, offset);
            return this;
        }

        @Override
        public Default unwrap() {
            super.unwrap();
            return this;
        }
    }

    class Mutable extends TypedHeader<Mutable> implements TimerHeader {

        private Mutable(final MutableDirectBuffer buffer) {
            super(Mutable.class, Type.TIMER, buffer);
        }

        public Mutable subtypeId(final short subtypeId) {
            super.subtypeId(TimerHeader.validateSubtypeId(subtypeId));
            return this;
        }

        public Mutable start(final int timerId) {
            subtypeId(validateTimerId(timerId));
            return this;
        }

        public Mutable stop(final int timerId) {
            subtypeId((short)(validateTimerId(timerId) | SUBTYPE_FLAG_TIMER_STOPPED));
            return this;
        }

        public Mutable expire(final int timerId) {
            subtypeId((short)(validateTimerId(timerId) | SUBTYPE_FLAG_TIMER_STOPPED | SUBTYPE_FLAG_TIMER_EXPIRED));
            return this;
        }

        public Mutable timeoutMillis(final int timeoutMillis) {
            if (timeoutMillis < 0) {
                throw new IllegalArgumentException("timeout cannot be negative: " + timeoutMillis);
            }
            return userData(timeoutMillis);
        }

        public Mutable timeoutMicros(final int timeoutMicros) {
            if (timeoutMicros < 0) {
                throw new IllegalArgumentException("timeout cannot be negative: " + timeoutMicros);
            }
            return userData(timeoutMicros | TIMEOUT_FLAG_MICRO);
        }
    }

    static Default create() {
        return new Default();
    }

    static Default create(final DirectBuffer source, final int offset) {
        return create().wrap(source, offset);
    }

    static Mutable allocate() {
        return new Mutable(MutableHeader.allocateBuffer());
    }

    static Mutable allocateDirect() {
        return new Mutable(MutableHeader.allocateDirectBuffer());
    }

    static short validateSubtypeId(final short subtypeId) {
        if ((subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) == 0 & (subtypeId & SUBTYPE_FLAG_TIMER_EXPIRED) != 0) {
            throw new IllegalArgumentException("Invalid subtype ID ('Stopped' flag is required if 'Expired' flags is set): " + subtypeId);
        }
        return subtypeId;
    }

    static short validateTimerId(final int timerId) {
        if ((timerId & SUBTYPE_MASK_TIMER_ID) == timerId) {
            return (short)timerId;
        }
        throw new IllegalArgumentException("Invalid timer ID: " + timerId);
    }
}
