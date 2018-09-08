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

public class DataHeader implements Header {

    private byte version;
    private short subtypeId;
    private int inputSourceId;
    private long sourceSeqNo;
    private long eventTimeNanosSinceEpoch;
    private int userData;
    private int payloadLength;

    @Override
    public byte version() {
        return version;
    }

    @Override
    public Type type() {
        return Type.DATA;
    }

    @Override
    public short subtypeId() {
        return subtypeId;
    }

    @Override
    public int inputSourceId() {
        return inputSourceId;
    }

    @Override
    public long sourceSeqNo() {
        return sourceSeqNo;
    }

    @Override
    public long eventTimeNanosSinceEpoch() {
        return eventTimeNanosSinceEpoch;
    }

    @Override
    public int userData() {
        return userData;
    }

    @Override
    public int payloadLength() {
        return payloadLength;
    }

    public DataHeader version(final Version version) {
        return version(version.code());
    }

    public DataHeader version(final byte version) {
        this.version = version;
        return this;
    }

    public DataHeader subtypeId(final short subtypeId) {
        this.subtypeId = subtypeId;
        return this;
    }

    public DataHeader inputSourceId(final int inputSourceId) {
        this.inputSourceId = inputSourceId;
        return this;
    }

    public DataHeader sourceSeqNo(final long sourceSeqNo) {
        this.sourceSeqNo = AdminHeader.validateSourceSeqNo(sourceSeqNo);
        return this;
    }

    public DataHeader eventTimeNanosSinceEpoch(final long eventTimeNanosSinceEpoch) {
        this.eventTimeNanosSinceEpoch = eventTimeNanosSinceEpoch;
        return this;
    }

    public DataHeader userData(final int userData) {
        this.userData = userData;
        return this;
    }

    public DataHeader payloadLength(final int payloadLength) {
        this.payloadLength = AdminHeader.validatePayloadLength(payloadLength);
        return this;
    }

    public DataHeader init(final Header header) {
        if (header.type() != Type.DATA) {
            throw new IllegalArgumentException("Not a 'DATA' type: " + header.type());
        }
        return this
                .version(header.version())
                .subtypeId(header.subtypeId())
                .inputSourceId(header.inputSourceId())
                .sourceSeqNo(header.sourceSeqNo())
                .eventTimeNanosSinceEpoch(header.eventTimeNanosSinceEpoch())
                .userData(header.userData())
                .payloadLength(header.payloadLength());
    }
}
