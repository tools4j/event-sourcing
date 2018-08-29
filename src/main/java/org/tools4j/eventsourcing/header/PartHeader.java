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
import org.tools4j.eventsourcing.event.Multipart;
import org.tools4j.eventsourcing.event.Type;

import java.util.EnumSet;
import java.util.Set;

public class PartHeader implements Multipart.Part {

    private static final Set<Type> ALLOWED_TYPES = EnumSet.of(Type.LEADERSHIP, Type.TIMER, Type.SHUTDOWN, Type.NOOP, Type.DATA);

    private Type type = Type.DATA;
    private short subtypeId;
    private int userData;
    private int payloadLength;

    @Override
    public Type type() {
        return type;
    }

    @Override
    public short subtypeId() {
        return subtypeId;
    }

    @Override
    public int userData() {
        return userData;
    }

    @Override
    public int payloadLength() {
        return payloadLength;
    }

    public PartHeader type(final Type type) {
        if (!ALLOWED_TYPES.contains(type)) {
            throw new IllegalArgumentException("Not a valid part type: " + type);
        }
        this.type = type;
        return this;
    }

    public PartHeader subtypeId(final short subtypeId) {
        this.subtypeId = subtypeId;
        return this;
    }

    public PartHeader userData(final int userData) {
        this.userData = userData;
        return this;
    }

    public PartHeader payloadLength(final int payloadLength) {
        this.payloadLength = AdminHeader.validatePayloadLength(payloadLength);
        return this;
    }

    public PartHeader init(final Header header) {
        return this
                .type(header.type())
                .subtypeId(header.subtypeId())
                .userData(header.userData())
                .payloadLength(header.payloadLength());
    }

    public PartHeader init(final Multipart.Part header) {
        return this
                .type(header.type())
                .subtypeId(header.subtypeId())
                .userData(header.userData())
                .payloadLength(header.payloadLength());
    }
}
