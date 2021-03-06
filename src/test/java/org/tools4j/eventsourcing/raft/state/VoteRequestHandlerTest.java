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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VoteRequestHandlerTest {
    @Mock
    private RaftLog persistentState;
    @Mock
    private Timer electionTimer;

    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private VoteResponseEncoder voteResponseEncoder = new VoteResponseEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

    @Mock
    private Publisher publisher;
    private int serverId = 1;

    @Mock
    private VoteRequestDecoder voteRequestDecoder;
    @Mock
    private HeaderDecoder headerDecoder;
    @Mock
    private LogKeyDecoder lastLogKeyDecoder;
    @Mock
    private Logger logger;


    private VoteRequestHandler voteRequestHandler;

    @Before
    public void setUp() throws Exception {
        when(voteRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteRequestDecoder.lastLogKey()).thenReturn(lastLogKeyDecoder);
        voteRequestHandler = new VoteRequestHandler(persistentState, electionTimer, messageHeaderEncoder, voteResponseEncoder, encoderBuffer, publisher, serverId);
    }

    @Test
    public void apply_grants_vote_and_transitions_to_follower_not_replaying_the_request_when_terms_are_equal_and_log_is_less_that_request_log() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(-1);
        when(persistentState.hasNotVotedYet()).thenReturn(true);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.T);
    }

    @Test
    public void apply_grants_vote_and_transitions_to_follower_not_replaying_the_request_when_terms_are_equal_and_log_is_equal_to_request_log() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(0);
        when(persistentState.hasNotVotedYet()).thenReturn(true);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.T);
    }

    @Test
    public void apply_rejects_vote_when_terms_are_equal_and_log_is_less_that_request_log_and_server_has_already_voted_another_leader() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final int anotherServer = 0;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(-1);
        when(persistentState.hasNotVotedYet()).thenReturn(false);
        when(persistentState.votedFor()).thenReturn(anotherServer);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.F);
    }

    @Test
    public void apply_grants_vote_when_terms_are_equal_and_log_is_less_that_request_log_and_server_has_already_voted_for_the_candidate() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(-1);
        when(persistentState.hasNotVotedYet()).thenReturn(false);
        when(persistentState.votedFor()).thenReturn(candidateId);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.T);
    }

    @Test
    public void apply_rejects_vote_when_terms_are_equal_and_log_is_greater_than_request_log() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(1);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.F);
    }

}