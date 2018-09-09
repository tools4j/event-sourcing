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
import org.tools4j.eventsourcing.command.CommandHandler;
import org.tools4j.eventsourcing.command.CommitCommands;
import org.tools4j.eventsourcing.command.TimerCommands;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.Objects;

public class MultipartCommandHandler implements CommandHandler.MultipartHandler {

    private final OutputQueue.Appender appender;
    private final MultipartCommitCommands multipartCommitCommands;
    private final TimerCommands timerCommands;
    private final AdminCommands adminCommands;

    public MultipartCommandHandler(final OutputQueue.Appender appender,
                                   final TimerCommands timerCommands,
                                   final AdminCommands adminCommands,
                                   final MultipartCommitCommands multipartCommitCommands) {
        this.appender = Objects.requireNonNull(appender);
        this.timerCommands = Objects.requireNonNull(timerCommands);
        this.adminCommands = Objects.requireNonNull(adminCommands);
        this.multipartCommitCommands = Objects.requireNonNull(multipartCommitCommands);
    }

    void startMultipart(final Header inputEventHeader) {
        multipartCommitCommands.startMultipart(inputEventHeader);
    }

    @Override
    public TimerCommands timerCommands() {
        return timerCommands;
    }

    @Override
    public AdminCommands adminCommands() {
        return adminCommands;
    }

    @Override
    public CommitCommands commitCommands() {
        return multipartCommitCommands;
    }

    @Override
    public void completeMultipart() {
        multipartCommitCommands.completeMultipart(appender);
    }
}
