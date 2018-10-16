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
package org.tools4j.eventsourcing.raft.state;

import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.tools4j.eventsourcing.sbe.AppendRequestDecoder;
import org.tools4j.eventsourcing.sbe.AppendResponseDecoder;
import org.tools4j.eventsourcing.sbe.VoteRequestDecoder;
import org.tools4j.eventsourcing.sbe.VoteResponseDecoder;

import java.util.Objects;

public class LoggingServerState implements ServerState {
    private final ServerState delegateServerState;
    private final StringBuilder stringBuilder;
    private final Logger logger;

    public LoggingServerState(final ServerState delegateServerState,
                              final StringBuilder stringBuilder,
                              final Logger logger) {
        this.delegateServerState = Objects.requireNonNull(delegateServerState);
        this.stringBuilder = Objects.requireNonNull(stringBuilder);
        this.logger = Objects.requireNonNull(logger);
    }

    @Override
    public Role role() {
        return delegateServerState.role();
    }

    @Override
    public void onTransition() {
        delegateServerState.onTransition();
    }

    @Override
    public Transition processTick() {
        return delegateServerState.processTick();
    }

    @Override
    public Transition onVoteRequest(final VoteRequestDecoder voteRequestDecoder) {
        stringBuilder.setLength(0);
        voteRequestDecoder.appendTo(stringBuilder);
        logger.info("onVoteRequest: {}", stringBuilder);
        return delegateServerState.onVoteRequest(voteRequestDecoder);
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        stringBuilder.setLength(0);
        voteResponseDecoder.appendTo(stringBuilder);
        logger.info("onVoteResponse: {}", stringBuilder);
        return delegateServerState.onVoteResponse(voteResponseDecoder);
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        stringBuilder.setLength(0);
        appendRequestDecoder.appendTo(stringBuilder);
        logger.info("onAppendRequest: {}", stringBuilder);
        return delegateServerState.onAppendRequest(appendRequestDecoder);
    }

    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        stringBuilder.setLength(0);
        appendResponseDecoder.appendTo(stringBuilder);
        logger.info("onAppendResponse: {}", stringBuilder);
        return delegateServerState.onAppendResponse(appendResponseDecoder);
    }

    @Override
    public void appendCommand(final int source, final long sourceSeq, final long timeNanos, final DirectBuffer buffer, final int offset, final int length) {
        delegateServerState.appendCommand(source, sourceSeq, timeNanos, buffer, offset, length);
    }

    @Override
    public Transition onTimeoutNow() {
        return delegateServerState.onTimeoutNow();
    }
}
