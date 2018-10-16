/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j, Marco Terzer, Anton Anufriev
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
package org.tools4j.eventsourcing;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

public class TestMessage {
    public static final int DEFAULT_TEST_MESSAGE_LENGTH = 51;

    public final DirectBuffer buffer;
    public final int offset;
    public final int length;

    public TestMessage(final DirectBuffer buffer, final int offset, final int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
    }

    public static TestMessage of(final DirectBuffer buffer, final int offset, final int length) {
        return new TestMessage(buffer, offset, length);
    }

    public static TestMessage forLength(final int length) {
        if (length < 3) throw new IllegalArgumentException("TestMessage length must not be less than 3");

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(length));
        buffer.putBytes(0, "#".getBytes());
        IntStream.range(1, length - 2).forEach(offset -> buffer.putBytes(offset, "-".getBytes()));
        buffer.putBytes(length - 2, "#\n".getBytes());

        return TestMessage.of(buffer, 0, length);
    }

    public static TestMessage forString(final String string) {

        final byte[] stringBytes = string.getBytes();

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(stringBytes.length));
        buffer.putBytes(0, stringBytes);

        return TestMessage.of(buffer, 0, stringBytes.length);
    }

    public static TestMessage forDefaultLength() {
        return forLength(DEFAULT_TEST_MESSAGE_LENGTH);
    }
}
