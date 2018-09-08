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
import org.tools4j.eventsourcing.application.CommitHandler;
import org.tools4j.eventsourcing.event.*;
import org.tools4j.eventsourcing.header.AdminHeader;
import org.tools4j.eventsourcing.header.MultipartHeader;
import org.tools4j.eventsourcing.header.PartHeader;
import org.tools4j.eventsourcing.store.OutputQueue;

public class MultipartCommitHandler implements CommitHandler {

    private final MultipartHeader header = new MultipartHeader();
    private final PartHeader partHeader = new PartHeader();
    private final Event event = new DefinedHeaderEvent(header);
    private final MutableDirectBuffer payload = new ExpandableDirectByteBuffer(1024);//make initial capacity configurable

    public MultipartCommitHandler() {
        this.header.version(Version.current());
    }

    public void startMultipart(final Event inputEvent) {
        startMultipart(inputEvent.header());
    }

    public void startMultipart(final Header inputEventHeader) {
        header
                .inputSourceId(inputEventHeader.inputSourceId())
                .sourceSeqNo(inputEventHeader.sourceSeqNo())
                .eventTimeNanosSinceEpoch(inputEventHeader.eventTimeNanosSinceEpoch())
                .partCount(0)
                .payloadLength(0)
        ;
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

    public void commitAdminEvent(final AdminHeader adminHeader) {
        partHeader
                .type(adminHeader.type())
                .subtypeId(adminHeader.subtypeId())
                .userData(adminHeader.userData())
                .payloadLength(0)
        ;
        appendPartHeader();
    }

    @Override
    public void commitEvent(final short subtypeId, final int userData, final DirectBuffer message, final int offset, final int length) {
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
        partHeader
                .type(Type.NOOP)
                .subtypeId(Header.DEFAULT_SUBTYPE_ID)
                .userData(userData)
                .payloadLength(0)
        ;
        appendPartHeader();
    }

    public void completeMultipart(final OutputQueue.Appender appender) {
        event.payload().wrap(payload, 0, header.payloadLength());
        appender.append(event);
    }
}
