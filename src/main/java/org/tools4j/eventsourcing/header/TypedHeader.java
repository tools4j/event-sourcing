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
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;

public class TypedHeader<T extends TypedHeader<T>> extends MutableHeader {

    private final T self;

    protected TypedHeader(final Class<T> typeClass, final Type type,
                          final MutableDirectBuffer buffer) {
        super(buffer);
        this.self = typeClass.cast(this);
        super.type(type);
    }

    @Override
    public T type(final Type type) {
        if (type != type()) {
            throw new IllegalArgumentException("Invalid type: " + type);
        }
        return self;
    }

    public T version(final Version version) {
        super.version(version);
        return self;
    }

    public T version(final byte version) {
        super.version(version);
        return self;
    }

    public T subtypeId(final short subtypeId) {
        super.subtypeId(subtypeId);
        return self;
    }

    public T inputSourceId(final int inputSourceId) {
        super.inputSourceId(inputSourceId);
        return self;
    }

    public T sourceSeqNo(final long sourceSeqNo) {
        super.sourceSeqNo(sourceSeqNo);
        return self;
    }

    public T eventTimeNanosSinceEpoch(final long eventTimeNanosSinceEpoch) {
        super.eventTimeNanosSinceEpoch(eventTimeNanosSinceEpoch);
        return self;
    }

    public T userData(final int userData) {
        super.userData(userData);
        return self;
    }

    public T payloadLength(final int payloadLength) {
        super.payloadLength(payloadLength);
        return self;
    }

    public T init(final Header header) {
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
}
