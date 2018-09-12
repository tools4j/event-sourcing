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

import org.tools4j.eventsourcing.command.CommitCommands;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.Objects;

public class SingleEventAppender implements OutputQueue.Appender {

    /**
     * State flow diagram:
     * <pre>
o --> (UNINITIALIZED) --> (SINGLE_STARTED) --> ((SINGLE_APPENDED))
                                |
                                v
                           (MULTI_STARTED) --> ((MULTI_APPENDED))
     * </pre>
     */
    private enum State {
        UNINITIALIZED,
        SINGLE_STARTED,
        SINGLE_APPENDED,
        MULTI_STARTED,
        MULTI_APPENDED
    }


    private final OutputQueue.Appender appender;
    private State state;

    public SingleEventAppender(final OutputQueue.Appender appender) {
        this.appender = Objects.requireNonNull(appender);
        this.state = State.UNINITIALIZED;
    }

    void startSingle() {
        this.state = State.SINGLE_STARTED;
    }

    void startMulti() {
        switch (state) {
            case SINGLE_STARTED:
                state = State.MULTI_STARTED;
                return;
            case SINGLE_APPENDED:
                throw new IllegalStateException("Cannot start multipart after appending an event");
            case MULTI_APPENDED:
                throw new IllegalStateException("Cannot start multipart after appending a multipart event");
            default:
                throw new IllegalStateException("Cannot start multipart in state " + state);
        }
    }

    void complete(final CommitCommands commitCommands) {
        try {
            switch (state) {
                case SINGLE_STARTED:
                    commitCommands.commitNoop(Header.DEFAULT_USER_DATA);
                    break;
                case MULTI_STARTED:
                    throw new IllegalStateException("Multipart has been started but not completed");
                case SINGLE_APPENDED:
                    //ok, nothing more required
                    break;
                case MULTI_APPENDED:
                    //ok, nothing more required
                    break;
                default:
                    throw new IllegalStateException("Invalid state: " + state);
            }
        } finally {
            state = State.UNINITIALIZED;
        }
    }

    @Override
    public long append(final Event event) {
        final State appendedState = appendedState();
        final long result = appender.append(event);
        state = appendedState;
        return result;
    }

    @Override
    public boolean compareAndAppend(final long expectedIndex, final Event event) {
        final State appendedState = appendedState();
        if (appender.compareAndAppend(expectedIndex, event)) {
            state = appendedState;
            return true;
        }
        return false;
    }

    private State appendedState() {
        switch (state) {
            case SINGLE_STARTED:
                return State.SINGLE_APPENDED;
            case MULTI_STARTED:
                return State.MULTI_APPENDED;
            case SINGLE_APPENDED://fallthrough
            case MULTI_APPENDED:
                throw new IllegalStateException("Only a single command can be appended, use multipart to append multiple commands");
            default:
                throw new IllegalStateException("Invalid state: " + state);
        }
    }
}
