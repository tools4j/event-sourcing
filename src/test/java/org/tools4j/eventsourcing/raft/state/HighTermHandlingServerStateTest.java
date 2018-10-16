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
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.sbe.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HighTermHandlingServerStateTest {

    @Mock
    private ServerState delegateServerState;
    @Mock
    private RaftLog raftLog;
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
    @Mock
    private HeaderDecoder headerDecoder;

    private HighTermHandlingServerState highTermHandlingServerState;


    @Before
    public void setUp() throws Exception {
        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(appendResponseDecoder.header()).thenReturn(headerDecoder);

        highTermHandlingServerState = new HighTermHandlingServerState(delegateServerState, raftLog, logger);
    }

    @Test
    public void role() throws Exception {
        //given
        when(delegateServerState.role()).thenReturn(Role.FOLLOWER);

        //when
        assertThat(highTermHandlingServerState.role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    public void onTransition() throws Exception {
        //when
        highTermHandlingServerState.onTransition();
        //then
        verify(delegateServerState).onTransition();
    }

    @Test
    public void processTick() throws Exception {
        //when
        highTermHandlingServerState.processTick();
        //then
        verify(delegateServerState).processTick();
    }

    @Test
    public void onVoteRequest_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onVoteRequest(voteRequestDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_REPLAY);
        verify(raftLog).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void onVoteRequest_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onVoteRequest(voteRequestDecoder);

        //then
        verify(raftLog, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void onVoteResponse_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onVoteResponse(voteResponseDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);
        verify(raftLog).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onVoteResponse(voteResponseDecoder);
    }

    @Test
    public void onVoteResponse_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onVoteResponse(voteResponseDecoder);

        //then
        verify(raftLog, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onVoteResponse(voteResponseDecoder);
    }

    @Test
    public void onAppendRequest_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onAppendRequest(appendRequestDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_REPLAY);
        verify(raftLog).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void onAppendRequest_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onAppendRequest(appendRequestDecoder);

        //then
        verify(raftLog, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void onAppendResponse_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onAppendResponse(appendResponseDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);
        verify(raftLog).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void onAppendResponse_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onAppendResponse(appendResponseDecoder);

        //then
        verify(raftLog, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void onTimeoutNow() throws Exception {
        //when
        highTermHandlingServerState.onTimeoutNow();
        //then
        verify(delegateServerState).onTimeoutNow();
    }

}