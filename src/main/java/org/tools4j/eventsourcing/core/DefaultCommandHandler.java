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
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.Objects;

public class DefaultCommandHandler implements CommandHandler {

    private final SingleEventAppender singleEventAppender;
    private final SingleCommitCommands singleCommitCommands;
    private final MultipartCommandHandler multipartCommandHandler;
    private final TimerCommands timerCommands;
    private final AdminCommands adminCommands;

    public DefaultCommandHandler(final SingleEventAppender singleEventAppender,
                                 final TimerCommands timerCommands,
                                 final AdminCommands adminCommands,
                                 final SingleCommitCommands singleCommitCommands,
                                 final MultipartCommandHandler multipartCommandHandler) {
        this.singleEventAppender = Objects.requireNonNull(singleEventAppender);
        this.timerCommands = Objects.requireNonNull(timerCommands);
        this.adminCommands = Objects.requireNonNull(adminCommands);
        this.singleCommitCommands = Objects.requireNonNull(singleCommitCommands);
        this.multipartCommandHandler = Objects.requireNonNull(multipartCommandHandler);
    }

    void initForAdminEvent(final long eventSeqNo, final long eventTimeNanosSinceEpoch) {
        singleEventAppender.init();
        singleCommitCommands.initForAdminEvent(eventSeqNo, eventTimeNanosSinceEpoch);
    }

    void initWithInputEvent(final Event inputEvent) {
        singleEventAppender.init();
        singleCommitCommands.initWithInputEvent(inputEvent);
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
        return singleCommitCommands;
    }

    @Override
    public MultipartHandler startMultipart() {
        singleEventAppender.initMultipart();
        singleCommitCommands.startMultipart(multipartCommandHandler);
        return multipartCommandHandler;
    }

    public static class SingleEventAppender implements OutputQueue.Appender {

        private static final int MULTIPART_SENTINEL = 2;
        private final OutputQueue.Appender appender;
        private int count;

        public SingleEventAppender(final OutputQueue.Appender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        void init() {
            this.count = 0;
        }

        void initMultipart() {
            if (count > 0) {
                if (count == MULTIPART_SENTINEL) {
                    throw new IllegalStateException("Multipart has already been started");
                }
                throw new IllegalStateException("Multipart cannot be started as a command has already been appended (hint: start multipart before appending events)");
            }
            this.count = MULTIPART_SENTINEL;
        }

        @Override
        public long append(final Event event) {
            checkEventCount();
            count++;
            return appender.append(event);
        }

        @Override
        public boolean compareAndAppend(final long expectedIndex, final Event event) {
            checkEventCount();
            if (appender.compareAndAppend(expectedIndex, event)) {
                count++;
                return true;
            }
            return false;
        }

        private void checkEventCount() {
            if (count > 0) {
                if (count == MULTIPART_SENTINEL) {
                    throw new IllegalStateException("Multipart has been started, use multipart handler to append commands");
                }
                throw new IllegalStateException("Only a single command can be appended (hint: use multipart to append multiple commands)");
            }
        }
    }
}
