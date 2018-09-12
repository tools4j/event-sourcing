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
package org.tools4j.eventsourcing.core;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.command.CommitCommands;
import org.tools4j.eventsourcing.event.DefaultEvent;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.MultipartHeader;
import org.tools4j.eventsourcing.header.MutableHeader;
import org.tools4j.eventsourcing.header.PartHeader;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.function.Consumer;

public class MultipartCommitCommands implements CommitCommands, Consumer<Header> {

    private final MultipartHeader.Mutable header = MultipartHeader.create(new UnsafeBuffer(0, 0));
    private final PartHeader partHeader = new PartHeader();
    private final DefaultEvent event = new DefaultEvent();
    private final MutableDirectBuffer payload = new ExpandableDirectByteBuffer(1024);//make initial capacity configurable

    public MultipartCommitCommands() {
        this.header.version(Version.current());
    }

    void init(final MutableHeader inputEventHeader) {
        header.buffer().wrap(inputEventHeader.buffer());
        header
                .type(Type.MULTIPART)
                .partCount(0)
                .payloadLength(0)
        ;
    }

    private void reset() {
        header.buffer().wrap(0, 0);
    }

    private void appendPart(final DirectBuffer message, final int offset, final int length) {
        final int targetOffset = header.payloadLength() + PartHeader.BYTE_LENGTH;
        message.getBytes(offset, payload, targetOffset, length);
        appendPartHeader();
    }

    private void appendPartHeader() {
        final int payloadLength = header.payloadLength();
        partHeader.writeTo(payload, payloadLength);
        header.partCount(header.partCount() + 1);
        header.payloadLength(payloadLength + PartHeader.BYTE_LENGTH + partHeader.payloadLength());
    }

    @Override
    public void accept(final Header noPayloadHeader) {
        checkStarted();
        partHeader
                .type(noPayloadHeader.type())
                .subtypeId(noPayloadHeader.subtypeId())
                .userData(noPayloadHeader.userData())
                .payloadLength(0)
        ;
        appendPartHeader();
    }

    @Override
    public void commitEvent(final short subtypeId, final int userData, final DirectBuffer message, final int offset, final int length) {
        checkStarted();
        partHeader
                .type(Type.DATA)
                .subtypeId(subtypeId)
                .userData(userData)
                .payloadLength(length)
        ;
        appendPart(message, offset, length);
    }

    @Override
    public void commitNoop(final int userData) {
        checkStarted();
        partHeader
                .type(Type.NOOP)
                .subtypeId(Header.DEFAULT_SUBTYPE_ID)
                .userData(userData)
                .payloadLength(0)
        ;
        appendPartHeader();
    }

    public void completeMultipart(final OutputQueue.Appender appender) {
        checkStarted();
        event.wrap(header, payload, 0);
        appender.append(event);
        reset();
    }

    private void checkStarted() {
        if (header.buffer().capacity() == 0) {
            throw new IllegalStateException("Multipart command has not been started");
        }
    }
}
