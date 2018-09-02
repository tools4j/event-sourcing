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
package org.tools4j.eventsourcing.event;

import org.agrona.MutableDirectBuffer;

import org.tools4j.eventsourcing.header.TimerHeader;

/**
 * Byte layout of a header:
 * <pre>
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    Version    |Type=MULTIPART |           Subtype ID          |  0- 3
     +---------------+---------------+-------------------------------+
     |                       Input Source ID                         |  4- 7
     +-------------------------------+-------------------------------+
     |0|                   Source Sequence Number                    |  8-15
     |                      (0 for admin events)                     |
     +-------------------------------+-------------------------------+
     |                          Event Time                           | 16-23
     |                   (nanoseconds since epoch)                   |
     +-------------------------------+-------------------------------+
     |                           User Data                           | 24-27
     +---------------+---------------+-------------------------------+
     |0|                       Payload Length                        | 28-31
     +---------------+---------------+-------------------------------+
   </pre>
 *
 * All types follow this format.  Interpretation of subtype and user data depends
 * on the type.
 *
 * @see Multipart
 * @see TimerHeader
 */
public interface Header {
    int BYTE_LENGTH = 32;
    int ADMIN_SOURCE_ID = 0;
    short DEFAULT_SUBTYPE_ID = 0;
    int DEFAULT_USER_DATA = 0;

    byte version();
    Type type();
    short subtypeId();
    int inputSourceId();
    long sourceSeqNo();
    long eventTimeNanosSinceEpoch();
    int userData();
    int payloadLength();

    default boolean isAdmin() {
        return inputSourceId() == ADMIN_SOURCE_ID;
    }

    default Header multipartParent() {
        return null;
    }

    default int writeTo(final MutableDirectBuffer target, final int offset) {
        if (offset + BYTE_LENGTH > target.capacity()) {
            throw new IllegalArgumentException("Not enough capacity: offset=" + offset + ", capacity=" + target.capacity());
        }
        target.putByte(offset + Offset.VERSION, version());
        target.putByte(offset + Offset.TYPE, type().code());
        target.putShort(offset + Offset.SUBTYPE_ID, subtypeId());
        target.putInt(offset + Offset.INPUT_SOURCE_ID, inputSourceId());
        target.putLong(offset + Offset.SOURCE_SEQ_NO, sourceSeqNo());
        target.putLong(offset + Offset.EVENT_TIME_NANOS_SINCE_EPOCH, eventTimeNanosSinceEpoch());
        target.putInt(offset + Offset.USER_DATA, userData());
        target.putInt(offset + Offset.PAYLOAD_LENGTH, payloadLength());
        return BYTE_LENGTH;
    }

    interface Offset {
        int VERSION = 0;
        int TYPE = 1;
        int SUBTYPE_ID = 2;
        int INPUT_SOURCE_ID = 4;
        int SOURCE_SEQ_NO = 8;
        int EVENT_TIME_NANOS_SINCE_EPOCH = 16;
        int USER_DATA = 24;
        int PAYLOAD_LENGTH = 28;
    }

}
