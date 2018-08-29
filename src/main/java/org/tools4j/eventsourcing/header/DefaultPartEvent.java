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
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Multipart;

public class DefaultPartEvent implements Event {

    private final DefaultPartHeader header = new DefaultPartHeader();
    private final DirectBuffer payload = new UnsafeBuffer(0, 0);

    public DefaultPartEvent wrap(final Header multipart, final DirectBuffer source, final int offset) {
        final int payloadLength = header.wrap(multipart, source, offset).payloadLength();
        payload.wrap(offset + Multipart.Part.BYTE_LENGTH, payloadLength);
        return this;
    }

    public DefaultPartEvent unwrap() {
        header.unwrap();
        payload.wrap(0, 0);
        return this;
    }

    @Override
    public DefaultPartHeader header() {
        return header;
    }

    @Override
    public DirectBuffer payload() {
        return payload;
    }

    @Override
    public int totalLength() {
        return Multipart.Part.BYTE_LENGTH + payloadLength();
    }
}
