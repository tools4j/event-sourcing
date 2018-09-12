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
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.eventsourcing.command.CommitCommands;
import org.tools4j.eventsourcing.event.DefaultEvent;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.header.MutableHeader;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.Objects;
import java.util.function.Consumer;

public class SingleCommitCommands implements CommitCommands, Consumer<Header> {

    private final MutableHeader header = new MutableHeader(new UnsafeBuffer(0, 0));
    private final DefaultEvent event = new DefaultEvent();
    private final OutputQueue.Appender appender;

    public SingleCommitCommands(final OutputQueue.Appender appender) {
        this.appender = Objects.requireNonNull(appender);
    }

    void init(final MutableHeader inputEventHeader) {
        header.buffer().wrap(inputEventHeader.buffer());
    }

    private void reset() {
        header.buffer().wrap(0, 0);
    }

    @Override
    public void accept(final Header noPayloadHeader) {
        headerInitialised()
                .type(noPayloadHeader.type())
                .subtypeId(noPayloadHeader.subtypeId())
                .userData(noPayloadHeader.userData())
                .payloadLength(0)
        ;
        event.wrap(header);
        appender.append(event);
        reset();
    }

    @Override
    public void commitEvent(final short subtypeId, final int userData, final DirectBuffer message, final int offset, final int length) {
        headerInitialised()
                .type(Type.DATA)
                .subtypeId(subtypeId)
                .userData(userData)
                .payloadLength(length)
        ;
        event.wrap(header, message, offset);
        appender.append(event);
        reset();
    }

    @Override
    public void commitNoop(final int userData) {
        headerInitialised()
                .type(Type.NOOP)
                .userData(userData)
        ;
        event.wrap(header);
        appender.append(event);
        reset();
    }

    private MutableHeader headerInitialised() {
        if (header.buffer().capacity() >= Header.BYTE_LENGTH) {
            return header;
        }
        throw new IllegalStateException("Commmit comands have not been initialised");
    }
}
