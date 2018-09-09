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
import org.tools4j.eventsourcing.command.CommitCommands;
import org.tools4j.eventsourcing.event.DefinedHeaderEvent;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.AdminHeader;
import org.tools4j.eventsourcing.header.DataHeader;
import org.tools4j.eventsourcing.header.NoopHeader;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.Objects;
import java.util.function.Consumer;

public class SingleCommitCommands implements CommitCommands, Consumer<AdminHeader> {

    private final DataHeader header = new DataHeader();
    private final NoopHeader noopHeader = new NoopHeader();
    private final DefinedHeaderEvent event = new DefinedHeaderEvent();
    private final OutputQueue.Appender appender;

    public SingleCommitCommands(final OutputQueue.Appender appender) {
        this.appender = Objects.requireNonNull(appender);
        this.header.version(Version.current());
    }

    void initWithInputEvent(final Event inputEvent) {
        initWithInputEvent(inputEvent.header());
    }

    void initWithInputEvent(final Header inputEventHeader) {
        header
                .inputSourceId(inputEventHeader.inputSourceId())
                .sourceSeqNo(inputEventHeader.sourceSeqNo())
                .eventTimeNanosSinceEpoch(inputEventHeader.eventTimeNanosSinceEpoch())
        ;
    }

    void initForAdminEvent(final long eventSeqNo, final long eventTimeNanosSinceEpoch) {
        header
                .inputSourceId(Header.ADMIN_SOURCE_ID)
                .sourceSeqNo(eventSeqNo)
                .eventTimeNanosSinceEpoch(eventTimeNanosSinceEpoch)
        ;
    }

    void startMultipart(final MultipartCommandHandler multipartCommandHandler) {
        multipartCommandHandler.startMultipart(header);
    }

    @Override
    public void accept(final AdminHeader adminHeader) {
        adminHeader
                .inputSourceId(header.inputSourceId())
                .sourceSeqNo(header.sourceSeqNo())
                .eventTimeNanosSinceEpoch(header.eventTimeNanosSinceEpoch())
        ;
        event
                .wrap(adminHeader)
                .payload().wrap(0, 0);
        appender.append(event);
    }

    @Override
    public void commitEvent(final short subtypeId, final int userData, final DirectBuffer message, final int offset, final int length) {
        header
                .subtypeId(subtypeId)
                .userData(userData)
                .payloadLength(length)
        ;
        event
                .wrap(header)
                .payload().wrap(message, offset, length);
        appender.append(event);
    }

    @Override
    public void commitNoop(final int userData) {
        accept(noopHeader.userData(userData));
    }
}
