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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;

import java.nio.ByteBuffer;
import java.util.Objects;

public class MutableHeader implements Header {

    private final MutableDirectBuffer buffer;

    public MutableHeader(final MutableDirectBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer);
    }

    public static MutableHeader allocate() {
        return new MutableHeader(allocateBuffer());
    }

    public static MutableHeader allocateDirect() {
        return new MutableHeader(allocateDirectBuffer());
    }

    protected static MutableDirectBuffer allocateBuffer() {
        return new UnsafeBuffer(new byte[Header.BYTE_LENGTH]);
    }

    protected static MutableDirectBuffer allocateDirectBuffer() {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(BYTE_LENGTH));
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
    public MutableDirectBuffer buffer() {
        return buffer;
    }

    public MutableHeader version(final Version version) {
        buffer.putByte(Offset.VERSION, version.code());
        return this;
    }

    public MutableHeader version(final byte version) {
        buffer.putByte(Offset.VERSION, Header.validateVersion(version));
        return this;
    }

    public MutableHeader type(final Type type) {
        buffer.putByte(Offset.TYPE, type.code());
        return this;
    }

    public MutableHeader subtypeId(final short subtypeId) {
        buffer.putShort(Offset.SUBTYPE_ID, subtypeId);
        return this;
    }

    public MutableHeader inputSourceId(final int inputSourceId) {
        buffer.putInt(Offset.INPUT_SOURCE_ID, inputSourceId);
        return this;
    }

    public MutableHeader sourceSeqNo(final long sourceSeqNo) {
        buffer.putLong(Offset.SOURCE_SEQ_NO, Header.validateSourceSeqNo(sourceSeqNo));
        return this;
    }

    public MutableHeader eventTimeNanosSinceEpoch(final long eventTimeNanosSinceEpoch) {
        buffer.putLong(Offset.EVENT_TIME_NANOS_SINCE_EPOCH, eventTimeNanosSinceEpoch);
        return this;
    }

    public MutableHeader userData(final int userData) {
        buffer.putLong(Offset.USER_DATA, userData);
        return this;
    }

    public MutableHeader payloadLength(final int payloadLength) {
        buffer.putLong(Offset.PAYLOAD_LENGTH, Header.validatePayloadLength(payloadLength));
        return this;
    }

    public MutableHeader init(final Header header) {
        return this
                .version(header.version())
                .type(header.type())
                .subtypeId(header.subtypeId())
                .inputSourceId(header.inputSourceId())
                .sourceSeqNo(header.sourceSeqNo())
                .eventTimeNanosSinceEpoch(header.eventTimeNanosSinceEpoch())
                .userData(header.userData())
                .payloadLength(header.payloadLength());
    }

    public MutableHeader reset() {
        buffer.setMemory(0, BYTE_LENGTH, (byte)0);
        return this;
    }
}
