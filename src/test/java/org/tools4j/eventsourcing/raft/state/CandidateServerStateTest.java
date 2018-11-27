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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.raft.timer.Timer;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.*;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CandidateServerStateTest {

    @Mock
    private RaftLog raftLog;
    @Mock
    private Peers peers;
    @Mock
    private Peer peer;
    @Mock
    private BiFunction<AppendRequestDecoder, Logger, Transition> appendRequestHandler;
    @Mock
    private Timer electionTimer;

    private int serverId = 1;

    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private VoteResponseDecoder voteResponseDecoder;
    @Mock
    private HeaderDecoder headerDecoder;

    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private VoteRequestEncoder voteRequestEncoder = new VoteRequestEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

    @Mock
    private Publisher publisher;

    private CandidateServerState candidateServerState;

    @Before
    public void setUp() throws Exception {
        candidateServerState = new CandidateServerState(raftLog,
                peers, appendRequestHandler, electionTimer, serverId,
                messageHeaderEncoder,
                voteRequestEncoder,
                encoderBuffer,
                publisher);
    }

    @Test
    public void role() throws Exception {
        assertThat(candidateServerState.role()).isEqualTo(Role.CANDIDATE);
    }

    @Test
    public void onTransition_should_start_new_election() throws Exception {
        assertNewElection(() -> candidateServerState.onTransition());
    }

    private void assertNewElection(final Runnable electionTrigger) {
        final int newTerm = 5;
        final int lastLogTerm = 4;
        final long lastLogIndex = 10;
        when(raftLog.clearVoteForAndIncCurrentTerm()).thenReturn(5);
        when(raftLog.currentTerm()).thenReturn(newTerm);
        when(raftLog.lastIndex()).thenReturn(lastLogIndex);
        when(raftLog.lastTerm()).thenReturn(lastLogTerm);

        electionTrigger.run();

        //then
        verify(electionTimer).restart();
        verify(raftLog).votedFor(serverId);
        verify(publisher).publish(encoderBuffer, 0, 32);

        final StringBuilder voteRequest = new StringBuilder();
        voteRequestEncoder.appendTo(voteRequest);

        assertThat(voteRequest)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + Peers.ALL)
                .contains("term=" + newTerm)
                .contains("lastLogKey=(term=" + lastLogTerm)
                .contains("index="+lastLogIndex);
    }

    @Test
    public void processTick_should_start_new_election_when_election_timer_has_elapsed() throws Exception {
        when(electionTimer.hasTimeoutElapsed()).thenReturn(true);
        assertNewElection(() -> candidateServerState.processTick());
    }

    @Test
    public void onAppendRequest_should_delegate_to_appendRequestHandler_when_requestTerm_is_lower_than_current_term() throws Exception {
        final int appendRequestTerm = 4;
        final int currentTerm = 5;

        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(appendRequestTerm);
        when(raftLog.currentTerm()).thenReturn(currentTerm);

        candidateServerState.onAppendRequest(appendRequestDecoder);

        verify(appendRequestHandler).apply(same(appendRequestDecoder), any(Logger.class));
    }

    @Test
    public void onAppendRequest_should_transition_to_follower_with_replay_when_requestTerm_is_equal_than_current_term() throws Exception {
        final int appendRequestTerm = 5;
        final int currentTerm = 5;

        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(appendRequestTerm);
        when(raftLog.currentTerm()).thenReturn(currentTerm);

        assertThat(candidateServerState.onAppendRequest(appendRequestDecoder)).isEqualTo(Transition.TO_FOLLOWER_REPLAY);

        verify(appendRequestHandler, times(0)).apply(same(appendRequestDecoder), any(Logger.class));
    }

    @Test
    public void onAppendRequest_should_transition_to_follower_with_replay_when_requestTerm_is_greater_than_current_term() throws Exception {
        final int appendRequestTerm = 6;
        final int currentTerm = 5;

        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(appendRequestTerm);
        when(raftLog.currentTerm()).thenReturn(currentTerm);

        assertThat(candidateServerState.onAppendRequest(appendRequestDecoder)).isEqualTo(Transition.TO_FOLLOWER_REPLAY);

        verify(appendRequestHandler, times(0)).apply(same(appendRequestDecoder), any(Logger.class));
    }

    @Test
    public void onVoteResponse_should_transition_to_leader_when_terms_match_and_vote_granted_and_majority_voted() throws Exception {
        final int responseTerm = 5;
        final int currentTerm = 5;
        final int followerId = 2;
        final BooleanType voteGranted = BooleanType.T;

        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(responseTerm);
        when(headerDecoder.sourceId()).thenReturn(followerId);
        when(voteResponseDecoder.voteGranted()).thenReturn(voteGranted);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(peers.peer(followerId)).thenReturn(peer);
        when(peers.majorityOfVotes()).thenReturn(true);

        assertThat(candidateServerState.onVoteResponse(voteResponseDecoder)).isEqualTo(Transition.TO_LEADER_NO_REPLAY);
        verify(peer).setGrantedVote(true);
    }

    @Test
    public void onVoteResponse_should_steady_transition_when_terms_match_and_vote_granted_and_no_majority_voted() throws Exception {
        final int responseTerm = 5;
        final int currentTerm = 5;
        final int followerId = 2;
        final BooleanType voteGranted = BooleanType.T;

        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(responseTerm);
        when(headerDecoder.sourceId()).thenReturn(followerId);
        when(voteResponseDecoder.voteGranted()).thenReturn(voteGranted);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(peers.peer(followerId)).thenReturn(peer);
        when(peers.majorityOfVotes()).thenReturn(false);

        assertThat(candidateServerState.onVoteResponse(voteResponseDecoder)).isEqualTo(Transition.STEADY);
        verify(peer).setGrantedVote(true);
    }

    @Test
    public void onVoteResponse_should_steady_transition_when_terms_match_and_vote_not_granted() throws Exception {
        final int responseTerm = 5;
        final int currentTerm = 5;
        final int followerId = 2;
        final BooleanType voteNotGranted = BooleanType.F;

        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(responseTerm);
        when(headerDecoder.sourceId()).thenReturn(followerId);
        when(voteResponseDecoder.voteGranted()).thenReturn(voteNotGranted);
        when(raftLog.currentTerm()).thenReturn(currentTerm);

        assertThat(candidateServerState.onVoteResponse(voteResponseDecoder)).isEqualTo(Transition.STEADY);
    }

    @Test
    public void onVoteResponse_should_steady_transition_when_terms_dont_match_and_vote_granted() throws Exception {
        final int responseTerm = 4;
        final int currentTerm = 5;
        final int followerId = 2;
        final BooleanType voteGranted = BooleanType.T;

        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.term()).thenReturn(responseTerm);
        when(headerDecoder.sourceId()).thenReturn(followerId);
        when(voteResponseDecoder.voteGranted()).thenReturn(voteGranted);
        when(raftLog.currentTerm()).thenReturn(currentTerm);

        assertThat(candidateServerState.onVoteResponse(voteResponseDecoder)).isEqualTo(Transition.STEADY);
    }

}