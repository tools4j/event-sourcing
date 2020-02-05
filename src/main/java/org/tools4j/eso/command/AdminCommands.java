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
package org.tools4j.eso.command;

import org.agrona.MutableDirectBuffer;

public enum AdminCommands {
    ;

    public static final int TIMER_TYPE_OFFSET = 0;
    public static final int TIMER_TYPE_LENGTH = Integer.BYTES;
    public static final int TIMER_ID_OFFSET = TIMER_TYPE_OFFSET + TIMER_TYPE_LENGTH;
    public static final int TIMER_ID_LENGTH = Long.BYTES;
    public static final int TIMER_TIMEOUT_OFFSET = TIMER_ID_OFFSET + TIMER_ID_LENGTH;
    public static final int TIMER_TIMEOUT_LENGTH = Long.BYTES;

    public static final int TIMER_PAYLOAD_SIZE = TIMER_TYPE_LENGTH + TIMER_ID_LENGTH +
            TIMER_TIMEOUT_LENGTH;


    public static int triggerTimer(final MutableDirectBuffer buffer,
                                   final int offset,
                                   final int timerType,
                                   final long timerId,
                                   final long timeout) {
        buffer.putInt(offset + TIMER_TYPE_OFFSET, timerType);
        buffer.putLong(offset + TIMER_ID_OFFSET, timerId);
        buffer.putLong(offset + TIMER_TIMEOUT_OFFSET, timeout);
        return TIMER_PAYLOAD_SIZE;
    }
}
