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
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;

public interface InitializeHeader extends Header {

    default int leaderId() {
        return userData();
    }

    class Default extends DefaultHeader implements InitializeHeader {
        @Override
        public Default wrap(final Header header) {
            super.wrap(header);
            return this;
        }

        @Override
        public Default wrap(final DirectBuffer source, final int offset) {
            super.wrap(source, offset);
            return this;
        }

        @Override
        public Default unwrap() {
            super.unwrap();
            return this;
        }
    }

    class Mutable extends TypedHeader<Mutable> implements InitializeHeader {
        private Mutable(final MutableDirectBuffer buffer) {
            super(Mutable.class, Type.INITIALIZE, buffer);
        }

        public Mutable leaderId(final int leaderId) {
            return userData(leaderId);
        }
    }

    static Default create() {
        return new Default();
    }

    static Default create(final DirectBuffer source, final int offset) {
        return create().wrap(source, offset);
    }

    static Mutable allocate() {
        return new Mutable(MutableHeader.allocateBuffer());
    }

    static Mutable allocateDirect() {
        return new Mutable(MutableHeader.allocateDirectBuffer());
    }
}
