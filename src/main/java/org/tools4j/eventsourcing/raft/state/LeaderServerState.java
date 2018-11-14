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
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.*;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongToIntFunction;

public class LeaderServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(Role.LEADER.name());

    private final RaftLog raftLog;
    private final Peers peers;
    private final int serverId;
    private final AppendRequestEncoder appendRequestEncoder;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final MutableDirectBuffer commandDecoderBuffer;
    private final RaftIndexDecoder raftIndexDecoder;

    private final NoopEncoder noopEncoder = new NoopEncoder();

    private final Publisher publisher;
    private final IntConsumer onLeaderTransitionHandler;
    private final int maxBatchSize;

    private final LongToIntFunction indexToTermLookup;
    private final Consumer<Peer> sendAppendRequestAndResetHeartbeatTimerForAll;
    private final Consumer<Peer> resetHeartbeatTimerForAll;



    public LeaderServerState(final RaftLog raftLog,
                             final Peers peers,
                             final int serverId,
                             final AppendRequestEncoder appendRequestEncoder,
                             final MessageHeaderEncoder messageHeaderEncoder,
                             final MutableDirectBuffer encoderBuffer,
                             final MutableDirectBuffer commandDecoderBuffer,
                             final Publisher publisher,
                             final IntConsumer onLeaderTransitionHandler,
                             final int maxBatchSize) {
        this.raftLog = Objects.requireNonNull(raftLog);
        this.peers = Objects.requireNonNull(peers);
        this.serverId = serverId;
        this.appendRequestEncoder = Objects.requireNonNull(appendRequestEncoder);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.commandDecoderBuffer = Objects.requireNonNull(commandDecoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.onLeaderTransitionHandler = Objects.requireNonNull(onLeaderTransitionHandler);
        this.maxBatchSize = maxBatchSize;
        this.indexToTermLookup = this.raftLog::term;

        this.raftIndexDecoder = new RaftIndexDecoder();

        this.sendAppendRequestAndResetHeartbeatTimerForAll = peer -> {
            sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), false);
            peer.heartbeatTimer().reset();
        };

        this.resetHeartbeatTimerForAll = peer -> peer.heartbeatTimer().reset();

    }


    @Override
    public Role role() {
        return Role.LEADER;
    }

    @Override
    public void onTransition() {
        LOGGER.info("Transitioned");
        peers.resetAsFollowers(raftLog.size());

        final int noopLength = noopEncoder.wrapAndApplyHeader(encoderBuffer, 0, messageHeaderEncoder).encodedLength() +
                messageHeaderEncoder.encodedLength();

        accept(0, 0, 0, encoderBuffer, 0, noopLength);
        sendAppendRequestToAllAndResetHeartbeatTimer();
        onLeaderTransitionHandler.accept(serverId);
    }

    @Override
    public void accept(final int source, final long sourceSeq, final long timeNanos, final DirectBuffer buffer, final int offset, final int length) {
        //LOGGER.info("Command received, length={}", length);
        raftLog.append(raftLog.currentTerm(), source, sourceSeq, timeNanos, buffer, offset, length);
        sendAppendRequestToAllAndResetHeartbeatTimer();
    }

    @Override
    public Transition processTick() {
        peers.forEach(peer -> {
            if (peer.heartbeatTimer().hasTimeoutElapsed()) {
                LOGGER.info("Heartbeat timer elapsed, send heartbeat to {}", peer.serverId());
                sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), false);
                peer.heartbeatTimer().reset();
            }
        });
        return Transition.STEADY;
    }


    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        final HeaderDecoder headerDecoder = appendResponseDecoder.header();
        final int sourceId = headerDecoder.sourceId();
        final long requestPrevLogIndex = appendResponseDecoder.prevLogIndex();

        final Peer peer = peers.peer(sourceId);
        final BooleanType successful = appendResponseDecoder.successful();
        if (successful == BooleanType.F) {
            //LOGGER.info("Unsuccessful appendResponse from server {}", sourceId);
            if (!peer.comparePreviousAndDecrementNextIndex(requestPrevLogIndex)) {
                //LOGGER.info("Unsuccessful appendResponse prevLogIndex {} does not match {} from server {}, awaiting newer response", requestPrevLogIndex, peer.previousIndex(), sourceId);
            } else {
                sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), true);
            }
        } else {
            //LOGGER.info("Successful appendResponse from server {}", sourceId);
            final long matchLogIndex = appendResponseDecoder.matchLogIndex();
            if (!peer.comparePreviousAndUpdateMatchAndNextIndex(requestPrevLogIndex, matchLogIndex)) {
                //LOGGER.info("Successful appendResponse prevLogIndex {} does not match {} from server {}, awaiting newer response", requestPrevLogIndex, peer.previousIndex(), sourceId);
            } else {
                if (peer.matchIndex() < raftLog.lastIndex()) {
                    sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), false);
                }
            }
        }
        peer.heartbeatTimer().reset();
        updateCommitIndex();
        return Transition.STEADY;
    }

    private void updateCommitIndex() {
        long currentCommitIndex = raftLog.commitIndex();
        int currentTerm = raftLog.currentTerm();

        //FIXME check term at NULL_INDEX
        long nextCommitIndex = peers.majorityCommitIndex(currentCommitIndex, currentTerm, indexToTermLookup);

        if (nextCommitIndex > currentCommitIndex) {
            //LOGGER.info("Update commit index {}", nextCommitIndex);
            raftLog.commitIndex(nextCommitIndex);
        }
    }

    private void sendAppendRequestToAllAndResetHeartbeatTimer() {
        final long matchIndexPrecedingNextIndex = peers.matchIndexPrecedingNextIndexAndEqualAtAllPeers();
        if (matchIndexPrecedingNextIndex != Peer.NULL_INDEX) {
            sendAppendRequest(Peers.ALL, matchIndexPrecedingNextIndex + 1, matchIndexPrecedingNextIndex, false);
            peers.forEach(resetHeartbeatTimerForAll);
        } else {
            peers.forEach(sendAppendRequestAndResetHeartbeatTimerForAll);
        }
    }

    private boolean sendAppendRequest(final int destinationId,
                                      final long nextIndex,
                                      final long matchIndex,
                                      final boolean empty) {

        final int currentTerm = raftLog.currentTerm();

        final long prevLogIndex = nextIndex - 1;
        final int termAtPrevLogIndex = raftLog.term(prevLogIndex);

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(AppendRequestEncoder.SCHEMA_ID)
                .version(AppendRequestEncoder.SCHEMA_VERSION)
                .blockLength(AppendRequestEncoder.BLOCK_LENGTH)
                .templateId(AppendRequestEncoder.TEMPLATE_ID)
                .encodedLength();

        appendRequestEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(destinationId)
                .sourceId(serverId)
                .term(currentTerm);

        appendRequestEncoder
                .commitLogIndex(raftLog.commitIndex())
                .prevLogKey()
                    .index(prevLogIndex)
                    .term(termAtPrevLogIndex);

        if (empty) {
            appendRequestEncoder.logEntriesCount(0);
        } else {
            long nextLogIndex = nextIndex;
            final long lastIndex = raftLog.lastIndex();

            if (matchIndex == prevLogIndex) {

                final long endOfBatchIndex = Long.min(prevLogIndex + maxBatchSize, lastIndex);

                final AppendRequestEncoder.LogEntriesEncoder logEntriesEncoder = appendRequestEncoder
                        .logEntriesCount((int) (endOfBatchIndex - prevLogIndex));

                while(nextLogIndex <= endOfBatchIndex) {

                    final int termAtNextLogIndex = raftLog.term(nextLogIndex);
                    raftLog.wrap(nextLogIndex, raftIndexDecoder, commandDecoderBuffer);
                    final int commandLength = commandDecoderBuffer.capacity();

                    logEntriesEncoder.next()
                            .term(termAtNextLogIndex)
                            .commandSource(raftIndexDecoder.source())
                            .commandSequence(raftIndexDecoder.sourceSeq())
                            .commandTimeNanos(raftIndexDecoder.eventTimeNanos())
                            .putCommand(commandDecoderBuffer, 0, commandLength);

                    nextLogIndex++;
                }
            } else {
                appendRequestEncoder.logEntriesCount(0);
            }
        }

        return publisher.publish(encoderBuffer, 0, headerLength + appendRequestEncoder.encodedLength());
    }
}
