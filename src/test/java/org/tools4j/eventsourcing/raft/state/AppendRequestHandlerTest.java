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
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.tools4j.eventsourcing.raft.api.LogContainment;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.raft.timer.Timer;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.*;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AppendRequestHandlerTest {
    @Mock
    private RaftLog raftLog;
    @Mock
    private Timer electionTimeout;
    private AppendResponseEncoder appendResponseEncoder = new AppendResponseEncoder();
    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));


    @Mock
    private Publisher publisher;
    private int serverId = 1;
    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private HeaderDecoder headerDecoder;
    @Mock
    private LogKeyDecoder prevLogKeyDecoder;
    @Mock
    private AppendRequestDecoder.LogEntriesDecoder logEntriesDecoder;
    @Mock
    private Logger logger;
    
    private AppendRequestHandler appendRequestHandler;

    @Before
    public void setUp() throws Exception {
        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(appendRequestDecoder.prevLogKey()).thenReturn(prevLogKeyDecoder);

        appendRequestHandler = new AppendRequestHandler(raftLog,
                electionTimeout, messageHeaderEncoder, appendResponseEncoder, encoderBuffer,
                publisher, serverId);
    }

    @Test
    public void apply_appends_logEntry_when_terms_are_equal_and_prevLogEntry_is_contained_and_nextLogEntry_is_not() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;
        final int commandSource = 10;
        final long commandSeq = 43543;
        final long commandTimeNanos = 54565;
        final int nextLogTerm = 2;
        final int nextCommandLength = 20;
        final int nextCommandLimit = 16;
        final int nextCommandOffset = nextCommandLimit + AppendRequestDecoder.LogEntriesDecoder.commandHeaderLength();
        final UnsafeBuffer nextCommandBuffer = new UnsafeBuffer();

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(raftLog.commitIndex()).thenReturn(-1L);

        when(raftLog.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.IN);
        when(raftLog.contains(nextLogIndex, nextLogTerm)).thenReturn(LogContainment.OUT);

        when(appendRequestDecoder.logEntries()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.iterator()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.hasNext()).thenReturn(true, false);
        when(logEntriesDecoder.next()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.term()).thenReturn(nextLogTerm);
        when(logEntriesDecoder.commandSource()).thenReturn(commandSource);
        when(logEntriesDecoder.commandSequence()).thenReturn(commandSeq);
        when(logEntriesDecoder.commandTimeNanos()).thenReturn(commandTimeNanos);
        when(logEntriesDecoder.commandLength()).thenReturn(nextCommandLength);
        when(appendRequestDecoder.limit()).thenReturn(nextCommandLimit);
        when(appendRequestDecoder.buffer()).thenReturn(nextCommandBuffer);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + nextLogIndex)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=T");


        verify(raftLog).append(nextLogTerm, commandSource, commandSeq ,commandTimeNanos, nextCommandBuffer, nextCommandOffset, nextCommandLength);
        verify(raftLog).commitIndex(nextLogIndex);
        verify(electionTimeout).restart();
    }


    @Test
    public void apply_skips_logEntry_when_terms_are_equal_and_prevLogEntry_and_next_logEntry_are_contained_in_server_log() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;
        final int nextLogTerm = 2;
        final int commandSource = 10;
        final long commandSeq = 43543;
        final long commandTimeNanos = 54565;

        final int nextCommandLength = 20;
        final int nextCommandLimit = 16;
        final int nextCommandOffset = nextCommandLimit + AppendRequestDecoder.LogEntriesDecoder.commandHeaderLength();
        final UnsafeBuffer nextCommandBuffer = new UnsafeBuffer();

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(raftLog.commitIndex()).thenReturn(-1L);

        when(raftLog.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.IN);
        when(raftLog.contains(nextLogIndex, nextLogTerm)).thenReturn(LogContainment.IN);

        when(appendRequestDecoder.logEntries()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.iterator()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.hasNext()).thenReturn(true, false);
        when(logEntriesDecoder.next()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.term()).thenReturn(nextLogTerm);
        when(logEntriesDecoder.commandSource()).thenReturn(commandSource);
        when(logEntriesDecoder.commandSequence()).thenReturn(commandSeq);
        when(logEntriesDecoder.commandTimeNanos()).thenReturn(commandTimeNanos);


        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + nextLogIndex)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=T");


        verify(raftLog, times(0)).append(nextLogTerm, commandSource, commandSeq ,commandTimeNanos, nextCommandBuffer, nextCommandOffset, nextCommandLength);
        verify(raftLog).commitIndex(nextLogIndex);
        verify(electionTimeout).restart();
    }

    @Test(expected = IllegalStateException.class)
    public void apply_throws_exception_logEntry_when_terms_are_equal_and_prevLogEntry_is_contained_and_next_log_index_conflicts() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;

        final int commandSource = 10;
        final long commandSeq = 43543;
        final long commandTimeNanos = 54565;


        final long nextLogIndex = prevLogIndex + 1;
        final int nextLogTerm = 2;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);

        when(raftLog.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.IN);
        when(raftLog.contains(nextLogIndex, nextLogTerm)).thenReturn(LogContainment.CONFLICT);

        when(appendRequestDecoder.logEntries()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.iterator()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.hasNext()).thenReturn(true, false);
        when(logEntriesDecoder.next()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.term()).thenReturn(nextLogTerm);
        when(logEntriesDecoder.commandSource()).thenReturn(commandSource);
        when(logEntriesDecoder.commandSequence()).thenReturn(commandSeq);
        when(logEntriesDecoder.commandTimeNanos()).thenReturn(commandTimeNanos);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);
    }

    @Test
    public void apply_unsuccessful_when_terms_are_equal_and_prevLogEntry_is_not_contained() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);

        when(raftLog.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.OUT);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + -1)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=F");


        verify(raftLog, times(0)).commitIndex(nextLogIndex);
        verify(electionTimeout).restart();
    }

    @Test
    public void apply_unsuccessful_truncates_log_to_prevLogIndex_when_terms_are_equal_and_prevLogEntry_is_conflicts() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);

        when(raftLog.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.CONFLICT);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + -1)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=F");


        verify(raftLog, times(0)).commitIndex(nextLogIndex);
        verify(raftLog).truncate(prevLogIndex);
        verify(electionTimeout).restart();
    }

    @Test
    public void apply_unsuccessful_when_request_term_less_than_current_term() throws Exception {
        //given
        final int requestTerm = 2;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(raftLog.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + -1)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=F");

        verify(electionTimeout, times(0)).restart();
    }
}