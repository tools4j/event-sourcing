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
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;

public class DefaultHeader implements Header {

    private final DirectBuffer buffer = new UnsafeBuffer(0, 0);

    public DefaultHeader wrap(final Header header) {
        return wrap(header.buffer(), 0);
    }

    public DefaultHeader wrap(final DirectBuffer source, final int offset) {
        buffer.wrap(source, offset, BYTE_LENGTH);
        return this;
    }

    public DefaultHeader unwrap() {
        buffer.wrap(0, 0);
        return this;
    }

    @Override
    public byte version() {
        return buffer.getByte(Offset.VERSION);
    }

    @Override
    public Type type() {
        return Type.valueByCode(buffer.getByte(Offset.TYPE));
    }

    @Override
    public short subtypeId() {
        return buffer.getShort(Offset.SUBTYPE_ID);
    }

    @Override
    public int inputSourceId() {
        return buffer.getInt(Offset.INPUT_SOURCE_ID);
    }

    @Override
    public long sourceSeqNo() {
        return buffer.getLong(Offset.SOURCE_SEQ_NO);
    }

    @Override
    public long eventTimeNanosSinceEpoch() {
        return buffer.getLong(Offset.EVENT_TIME_NANOS_SINCE_EPOCH);
    }

    @Override
    public int userData() {
        return buffer.getInt(Offset.USER_DATA);
    }

    @Override
    public int payloadLength() {
        return buffer.getInt(Offset.PAYLOAD_LENGTH);
    }

    @Override
    public DirectBuffer buffer() {
        return buffer;
    }
}
