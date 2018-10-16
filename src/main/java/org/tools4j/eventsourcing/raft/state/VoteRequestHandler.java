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
import org.slf4j.Logger;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.raft.timer.Timer;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.*;

import java.util.Objects;
import java.util.function.BiFunction;

public class VoteRequestHandler implements BiFunction<VoteRequestDecoder, Logger, Transition> {
    private final RaftLog raftLog;
    private final Timer electionTimer;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final VoteResponseEncoder voteResponseEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publisher publisher;
    private final int serverId;


    public VoteRequestHandler(final RaftLog raftLog,
                              final Timer electionTimer,
                              final MessageHeaderEncoder messageHeaderEncoder,
                              final VoteResponseEncoder voteResponseEncoder,
                              final MutableDirectBuffer encoderBuffer,
                              final Publisher publisher,
                              final int serverId) {
        this.raftLog = Objects.requireNonNull(raftLog);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.voteResponseEncoder = Objects.requireNonNull(voteResponseEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.serverId = serverId;
    }

    @Override
    public Transition apply(final VoteRequestDecoder voteRequestDecoder, final Logger logger) {
        final HeaderDecoder header = voteRequestDecoder.header();
        final int requestTerm = header.term();
        final int candidateId = header.sourceId();
        final LogKeyDecoder lastLogKey = voteRequestDecoder.lastLogKey();
        final long requestLastLogIndex = lastLogKey.index();
        final int requestLastLogTerm = lastLogKey.term();

        final Transition transition;
        final boolean granted;
        if (raftLog.currentTerm() <= requestTerm && raftLog.lastKeyCompareTo(requestLastLogIndex, requestLastLogTerm) <= 0) {
            logger.info("Current term <= requestTerm and persisted log lesser than log from source");
            if (raftLog.hasNotVotedYet()) {
                logger.info("Have not voted yet");
                raftLog.votedFor(candidateId);
                transition = Transition.TO_FOLLOWER_NO_REPLAY;
                granted = true;
            } else {
                final int voteFor = raftLog.votedFor();
                logger.info("Already voted, reject vote request if not the same, {} = {}", voteFor, candidateId);
                granted = voteFor == candidateId;
                transition = Transition.STEADY;
            }
        } else {
            logger.info("Rejecting votedFor as current term > requestTerm or persisted log bigger than log from source");
            granted = false;
            transition = Transition.STEADY;
        }
        if (granted) {
            electionTimer.restart();
        }

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(VoteResponseEncoder.SCHEMA_ID)
                .version(VoteResponseEncoder.SCHEMA_VERSION)
                .blockLength(VoteResponseEncoder.BLOCK_LENGTH)
                .templateId(VoteResponseEncoder.TEMPLATE_ID)
                .encodedLength();

        voteResponseEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(candidateId)
                .sourceId(serverId)
                .term(raftLog.currentTerm());

        voteResponseEncoder
                .voteGranted(granted ? BooleanType.T : BooleanType.F);

        publisher.publish(encoderBuffer, 0, headerLength + voteResponseEncoder.encodedLength());
        return transition;
    }
}
