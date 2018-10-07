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

    public static TestMessage forDefaultLength() {
        return forLength(DEFAULT_TEST_MESSAGE_LENGTH);
    }
}
