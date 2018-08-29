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

import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;

import java.util.concurrent.TimeUnit;

/**
 * Byte layout for a timeout header:
 * <pre>
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    Version    |      Type     |S|E|         Timer ID          |   0- 31
     +---------------+---------------+-------------------------------+
     |                       Input Source ID                         |  32- 63
     +-------------------------------+-------------------------------+
     |0|                   Source Sequence Number                    |  64-127
     |                      (0 for admin events)                     |
     +-------------------------------+-------------------------------+
     |                          Event Time                           | 128-191
     |                   (nanoseconds since epoch)                   |
     +-------------------------------+-------------------------------+
     |U|                   Timeout Millis/Micros                     | 192-223
     +---------------+---------------+-------------------------------+
     |                               0                               | 224-255
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
public class TimerHeader extends AdminHeader {

    public static final int SUBTYPE_FLAG_TIMER_STOPPED = 0x8000;
    public static final int SUBTYPE_FLAG_TIMER_EXPIRED = 0x4000;
    public static final int SUBTYPE_MASK_TIMER_ID      = 0x3fff;
    public static final short TIMER_ID_MIN = 0;
    public static final short TIMER_ID_MAX = SUBTYPE_MASK_TIMER_ID - 1;
    public static final int TIMEOUT_FLAG_MICRO = 0x80000000;
    public static final int TIMEOUT_MIN = 0;
    public static final int TIMEOUT_MAX = Integer.MAX_VALUE;

    private short subtypeId;

    public TimerHeader() {
        super(Type.TIMER);
    }

    @Override
    public short subtypeId() {
        return subtypeId;
    }

    public short timerId() {
        return (short) (subtypeId & SUBTYPE_MASK_TIMER_ID);
    }

    public boolean isStarted() {
        return (subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) == 0;
    }

    public boolean isStopped() {
        return (subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) != 0 & (subtypeId & SUBTYPE_FLAG_TIMER_EXPIRED) == 0;
    }

    public boolean isExpired() {
        return (subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) != 0 & (subtypeId & SUBTYPE_FLAG_TIMER_EXPIRED) != 0;
    }

    public boolean isMicros() {
        return (userData() & TIMEOUT_FLAG_MICRO) != 0;
    }

    public boolean isMillis() {
        return (userData() & TIMEOUT_FLAG_MICRO) == 0;
    }

    public int timeoutValue() {
        return userData() & (~TIMEOUT_FLAG_MICRO);
    }

    public long timeoutMicros() {
        final long timeout = userData();
        if ((timeout & TIMEOUT_FLAG_MICRO) != 0) {
            return timeout & (~TIMEOUT_FLAG_MICRO);
        }
        return TimeUnit.MILLISECONDS.toMicros(timeout);
    }

    @Override
    public TimerHeader version(final Version version) {
        super.version(version);
        return this;
    }

    @Override
    public TimerHeader version(final byte version) {
        super.version(version);
        return this;
    }

    public TimerHeader subtypeId(final short subtypeId) {
        if ((subtypeId & SUBTYPE_FLAG_TIMER_STOPPED) == 0 & (subtypeId & SUBTYPE_FLAG_TIMER_EXPIRED) != 0) {
            throw new IllegalArgumentException("Invalid subtype ID ('Stopped' flag is required if 'Expired' flags is set): " + subtypeId);
        }
        this.subtypeId = subtypeId;
        return this;
    }

    public TimerHeader start(final int timerId) {
        this.subtypeId = validateTimerId(timerId);
        return this;
    }

    public TimerHeader stop(final int timerId) {
        this.subtypeId = (short)(validateTimerId(timerId) | SUBTYPE_FLAG_TIMER_STOPPED);
        return this;
    }

    public TimerHeader expire(final int timerId) {
        this.subtypeId = (short)(validateTimerId(timerId) | SUBTYPE_FLAG_TIMER_STOPPED | SUBTYPE_FLAG_TIMER_EXPIRED);
        return this;
    }

    @Override
    public TimerHeader sourceSeqNo(final long sourceSeqNo) {
        super.sourceSeqNo(sourceSeqNo);
        return this;
    }

    @Override
    public TimerHeader eventTimeNanosSinceEpoch(final long eventTimeNanosSinceEpoch) {
        super.eventTimeNanosSinceEpoch(eventTimeNanosSinceEpoch);
        return this;
    }

    @Override
    public TimerHeader userData(final int userData) {
        super.userData(userData);
        return this;
    }

    public TimerHeader timeoutMillis(final int timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeout cannot be negative: " + timeoutMillis);
        }
        return userData(timeoutMillis);
    }

    public TimerHeader timeoutMicros(final int timeoutMicros) {
        if (timeoutMicros < 0) {
            throw new IllegalArgumentException("timeout cannot be negative: " + timeoutMicros);
        }
        return userData(timeoutMicros | TIMEOUT_FLAG_MICRO);
    }

    @Override
    public TimerHeader init(final Header header) {
        super.init(header);
        return subtypeId(header.subtypeId());
    }

    static short validateTimerId(final int timerId) {
        if ((timerId & SUBTYPE_MASK_TIMER_ID) == timerId) {
            return (short)timerId;
        }
        throw new IllegalArgumentException("Invalid timer ID: " + timerId);
    }

}
