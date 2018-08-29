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
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Multipart;
import org.tools4j.eventsourcing.event.Type;

public class DefaultPartHeader implements Header, Multipart.Part {

    private final MultipartHeader parent = new MultipartHeader();
    private final DirectBuffer buffer = new UnsafeBuffer(0, 0);

    public DefaultPartHeader wrap(final Header multipart, final DirectBuffer source, final int offset) {
        parent.init(multipart);
        buffer.wrap(source, offset, Multipart.Part.BYTE_LENGTH);
        return this;
    }

    public DefaultPartHeader unwrap() {
        buffer.wrap(0, 0);
        return this;
    }

    @Override
    public byte version() {
        return parent.version();
    }

    @Override
    public Type type() {
        return Type.valueByCode(buffer.getByte(Multipart.Part.Offset.TYPE));
    }

    @Override
    public short subtypeId() {
        return buffer.getShort(Multipart.Part.Offset.SUBTYPE_ID);
    }

    @Override
    public int inputSourceId() {
        return parent.inputSourceId();
    }

    @Override
    public long sourceSeqNo() {
        return parent.sourceSeqNo();
    }

    @Override
    public long eventTimeNanosSinceEpoch() {
        return parent.eventTimeNanosSinceEpoch();
    }

    @Override
    public int userData() {
        return buffer.getInt(Multipart.Part.Offset.USER_DATA);
    }

    @Override
    public int payloadLength() {
        return buffer.getInt(Multipart.Part.Offset.PAYLOAD_LENGTH);
    }

    @Override
    public Multipart multipartParent() {
        return parent;
    }

    @Override
    public int writeTo(MutableDirectBuffer target, int offset) {
        return Multipart.Part.super.writeTo(target, offset);
    }
}
