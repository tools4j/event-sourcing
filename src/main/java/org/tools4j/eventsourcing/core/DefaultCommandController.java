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

import org.tools4j.eventsourcing.command.AdminCommands;
import org.tools4j.eventsourcing.command.CommitCommands;
import org.tools4j.eventsourcing.command.TimerCommands;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.MutableHeader;

import java.util.Objects;

public class DefaultCommandController implements CommandController {

    private final MutableHeader header = MutableHeader.allocateDirect();
    private final DefaultSingleCommandHandler singleCommandHandler;
    private final MultipartCommandHandler multipartCommandHandler;
    private final SingleEventAppender singleEventAppender;

    public DefaultCommandController(final DefaultSingleCommandHandler singleCommandHandler,
                                    final MultipartCommandHandler multipartCommandHandler,
                                    final SingleEventAppender singleEventAppender) {
        this.singleCommandHandler = Objects.requireNonNull(singleCommandHandler);
        this.multipartCommandHandler = Objects.requireNonNull(multipartCommandHandler);
        this.singleEventAppender = Objects.requireNonNull(singleEventAppender);
    }

    public Provider provider() {
        return inputEvent -> {
            final Header inputHeader = inputEvent.header();
            header
                    .reset()
                    .version(Version.current())
                    .inputSourceId(inputHeader.inputSourceId())
                    .sourceSeqNo(inputHeader.sourceSeqNo())
                    .eventTimeNanosSinceEpoch(inputHeader.eventTimeNanosSinceEpoch())
            ;
            singleEventAppender.startSingle();
            singleCommandHandler.init(header);
            return DefaultCommandController.this;
        };
    }

    @Override
    public MultipartHandler startMultipart() {
        singleEventAppender.startMulti();
        return multipartCommandHandler.init(header);
    }

    @Override
    public TimerCommands timerCommands() {
        return singleCommandHandler.timerCommands();
    }

    @Override
    public AdminCommands adminCommands() {
        return singleCommandHandler.adminCommands();
    }

    @Override
    public CommitCommands commitCommands() {
        return singleCommandHandler.commitCommands();
    }

    @Override
    public void close() {
        singleEventAppender.complete(commitCommands());
    }
}
