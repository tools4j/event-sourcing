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

/**
 * <pre>
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    Version    |      Type     |           Template ID         |   0- 31
     +---------------+---------------+-------------------------------+
     |                       Input Source ID                         |  32- 63
     +-------------------------------+-------------------------------+
     |1|                   Source Sequence Number                    |  64-127
     |                      (0 for admin events)                     |
     +-------------------------------+-------------------------------+
     |                          Event Time                           | 128-191
     |                      (nanos since epoch)                      |
     +-------------------------------+-------------------------------+
     |                           User Data                           | 192-223
     +---------------+---------------+-------------------------------+
     |1|                       Payload Length                        | 224-255
     +---------------+---------------+-------------------------------+
   </pre>
 */
public interface Header {
    int BYTE_LENGTH = 32;
    int ADMIN_SOURCE_ID = 0;
    short ADMIN_TEMPLATE_ID = 0;

    byte version();
    Type type();
    short templateId();
    int inputSourceId();
    long sourceSeqNo();
    long eventTimeNanosSinceEpoch();
    int userData();
    int payloadLength();

    default int writeTo(final MutableDirectBuffer target, final int offset) {
        if (offset + BYTE_LENGTH > target.capacity()) {
            throw new IllegalArgumentException("Not enough capacity: offset=" + offset + ", capacity=" + target.capacity());
        }
        target.putByte(offset + Offset.VERSION, version());
        target.putByte(offset + Offset.TYPE, type().code());
        target.putShort(offset + Offset.TEMPLATE_ID, templateId());
        target.putInt(offset + Offset.INPUT_SOURCE_ID, inputSourceId());
        target.putLong(offset + Offset.SOURCE_SEQ_NO, sourceSeqNo());
        target.putLong(offset + Offset.EVENT_TIME_NANOS_SINCE_EPOCH, eventTimeNanosSinceEpoch());
        target.putInt(offset + Offset.USER_DATA, userData());
        target.putInt(offset + Offset.PAYLOAD_LENGTH, payloadLength());
        return offset + BYTE_LENGTH;
    }

    interface Offset {
        int VERSION = 0;
        int TYPE = 1;
        int TEMPLATE_ID = 2;
        int INPUT_SOURCE_ID = 4;
        int SOURCE_SEQ_NO = 8;
        int EVENT_TIME_NANOS_SINCE_EPOCH = 16;
        int USER_DATA = 24;
        int PAYLOAD_LENGTH = 28;
    }

}
