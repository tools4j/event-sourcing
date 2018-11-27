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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.tools4j.eventsourcing.sbe.AppendRequestDecoder;
import org.tools4j.eventsourcing.sbe.AppendResponseDecoder;
import org.tools4j.eventsourcing.sbe.VoteRequestDecoder;
import org.tools4j.eventsourcing.sbe.VoteResponseDecoder;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LoggingServerStateTest {
    @Mock
    private ServerState delegateServerState;
    private StringBuilder stringBuilder = new StringBuilder();
    @Mock
    private Logger logger;
    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private AppendResponseDecoder appendResponseDecoder;
    @Mock
    private VoteRequestDecoder voteRequestDecoder;
    @Mock
    private VoteResponseDecoder voteResponseDecoder;

    private LoggingServerState serverState;

    @Before
    public void setUp() throws Exception {
        serverState = new LoggingServerState(delegateServerState, stringBuilder, logger);
    }

    @Test
    public void role() throws Exception {
        serverState.role();
        verify(delegateServerState).role();
    }

    @Test
    public void onTransition() throws Exception {
        serverState.onTransition();
        verify(delegateServerState).onTransition();
    }

    @Test
    public void processTick() throws Exception {
        serverState.processTick();
        verify(delegateServerState).processTick();
    }

    @Test
    public void onVoteRequest() throws Exception {
        serverState.onVoteRequest(voteRequestDecoder);
        verify(delegateServerState).onVoteRequest(voteRequestDecoder);
        verify(voteRequestDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onVoteResponse() throws Exception {
        serverState.onVoteResponse(voteResponseDecoder);
        verify(delegateServerState).onVoteResponse(voteResponseDecoder);
        verify(voteResponseDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onAppendRequest() throws Exception {
        serverState.onAppendRequest(appendRequestDecoder);
        verify(delegateServerState).onAppendRequest(appendRequestDecoder);
        verify(appendRequestDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onAppendResponse() throws Exception {
        serverState.onAppendResponse(appendResponseDecoder);
        verify(delegateServerState).onAppendResponse(appendResponseDecoder);
        verify(appendResponseDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onTimeoutNow() throws Exception {
        serverState.onTimeoutNow();
        verify(delegateServerState).onTimeoutNow();
    }

}