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
 * Byte layout for a multipart message:
 * <pre>
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    Version    |      Type     |           Subtype ID          |  0- 3
     +---------------+---------------+-------------------------------+
     |                       Input Source ID                         |  4- 7
     +-------------------------------+-------------------------------+
     |0|                   Source Sequence Number                    |  8-15
     |                      (0 for admin events)                     |
     +-------------------------------+-------------------------------+
     |                          Event Time                           | 16-23
     |                   (nanoseconds since epoch)                   |
     +-------------------------------+-------------------------------+
     |                        Number of Parts                        | 24-27
     +---------------+---------------+-------------------------------+
     |0|                       Payload Length                        | 28-31
     +---------------+---------------+-------------------------------+
 </pre>
 *
 * Each part has a mini header:
 * <pre>
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    (unused)   |      Type     |           Subtype ID          |  0- 3
     +---------------+---------------+-------------------------------+
     |                           User Data                           |  4- 7
     +---------------+---------------+-------------------------------+
     |0|                     Part Payload Length                     |  8-11
     +---------------+---------------+-------------------------------+
   </pre>
 *
 * @see Header
 */
public interface Multipart extends Header {

    int partCount();

    interface Part {
        int BYTE_LENGTH = 12;
        Type type();
        short subtypeId();
        int userData();
        int payloadLength();

        default int writeTo(final MutableDirectBuffer target, final int offset) {
            if (offset + BYTE_LENGTH > target.capacity()) {
                throw new IllegalArgumentException("Not enough capacity: offset=" + offset + ", capacity=" + target.capacity());
            }
            target.putByte(offset + Offset.UNUSED, (byte)0);
            target.putByte(offset + Offset.TYPE, type().code());
            target.putShort(offset + Offset.SUBTYPE_ID, subtypeId());
            target.putInt(offset + Offset.USER_DATA, userData());
            target.putInt(offset + Offset.PAYLOAD_LENGTH, payloadLength());
            return BYTE_LENGTH;
        }

        interface Offset {
            int UNUSED = 0;
            int TYPE = 1;
            int SUBTYPE_ID = 2;
            int USER_DATA = 4;
            int PAYLOAD_LENGTH = 8;
        }
    }
}
