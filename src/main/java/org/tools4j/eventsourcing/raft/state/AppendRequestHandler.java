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

public class AppendRequestHandler implements BiFunction<AppendRequestDecoder, Logger, Transition> {
    private final RaftLog raftLog;
    private final Timer electionTimeout;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final AppendResponseEncoder appendResponseEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publisher publisher;
    private final int serverId;

    public AppendRequestHandler(final RaftLog raftLog,
                                final Timer electionTimeout,
                                final MessageHeaderEncoder messageHeaderEncoder,
                                final AppendResponseEncoder appendResponseEncoder,
                                final MutableDirectBuffer encoderBuffer,
                                final Publisher publisher,
                                final int serverId) {
        this.raftLog = Objects.requireNonNull(raftLog);
        this.electionTimeout = Objects.requireNonNull(electionTimeout);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.appendResponseEncoder = Objects.requireNonNull(appendResponseEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.serverId = serverId;
    }

    @Override
    public Transition apply(final AppendRequestDecoder appendRequestDecoder, final Logger logger) {
        final HeaderDecoder header = appendRequestDecoder.header();
        final int appendRequestTerm = header.term();
        final int leaderId = header.sourceId();
        final int currentTerm = raftLog.currentTerm();

        final LogKeyDecoder prevLogKeyDecoder = appendRequestDecoder.prevLogKey();
        final long requestPrevIndex = prevLogKeyDecoder.index();
        final int requestPrevTermAtIndex = prevLogKeyDecoder.term();

        final boolean successful;
        long matchLogIndex = -1;

        if (appendRequestTerm < currentTerm) {
            successful = false;
        } else {
            final long leaderCommitIndex = appendRequestDecoder.commitLogIndex();

            final RaftLog.Containment containment = raftLog.contains(requestPrevIndex, requestPrevTermAtIndex);

            switch (containment) {
                case IN:
                    matchLogIndex = appendToLog(requestPrevIndex, appendRequestDecoder, logger);

                    //From paper:  If leaderCommit > commitIndex, set commitIndex =
                    // min(leaderCommit, index of last new entry).
                    // I think, "index of last new entry" implies not empty raftLog entries.
                    if (leaderCommitIndex > raftLog.commitIndex()) {
                        raftLog.commitIndex(Long.min(leaderCommitIndex, matchLogIndex));
                    }

                    successful = true;
                    break;
                case OUT:
                    successful = false;
                    break;
                case CONFLICT:
                    raftLog.truncate(requestPrevIndex);
                    successful = false;
                    break;
                default:
                    throw new IllegalStateException("Unknown RaftLog.Containment " + containment);
            }
            electionTimeout.restart();
        }

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(AppendResponseEncoder.SCHEMA_ID)
                .version(AppendResponseEncoder.SCHEMA_VERSION)
                .blockLength(AppendResponseEncoder.BLOCK_LENGTH)
                .templateId(AppendResponseEncoder.TEMPLATE_ID)
                .encodedLength();

        appendResponseEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(leaderId)
                .sourceId(serverId)
                .term(currentTerm);

        appendResponseEncoder
                .matchLogIndex(matchLogIndex)
                .prevLogIndex(requestPrevIndex)
                .successful(successful ? BooleanType.T : BooleanType.F);

        publisher.publish(encoderBuffer, 0, headerLength + appendResponseEncoder.encodedLength());
        return Transition.STEADY;
    }

    private long appendToLog(final long prevLogIndex, final AppendRequestDecoder appendRequestDecoder, final Logger logger) {
        long nextIndex = prevLogIndex;

        for (final AppendRequestDecoder.LogEntriesDecoder logEntryDecoder : appendRequestDecoder.logEntries()) {
            nextIndex++;
            final int nextTermAtIndex = logEntryDecoder.term();
            final int commandSource = logEntryDecoder.commandSource();
            final long commandSequence = logEntryDecoder.commandSequence();
            final long commandTimeNanos = logEntryDecoder.commandTimeNanos();
            final int commandHeaderLength = AppendRequestDecoder.LogEntriesDecoder.commandHeaderLength();
            final int offset = appendRequestDecoder.limit() + commandHeaderLength;
            final int length = logEntryDecoder.commandLength();

            final RaftLog.Containment containment = raftLog.contains(nextIndex, nextTermAtIndex);
            switch (containment) {
                case OUT:
                    raftLog.append(nextTermAtIndex, commandSource, commandSequence, commandTimeNanos,
                            appendRequestDecoder.buffer(),
                            offset,
                            length);
                    //logger.info("Appended index {}, term {}, offset {}, length {}", nextIndex, nextTermAtIndex, offset, length);
                    break;
                case IN:
                    break;
                case CONFLICT:
                    raftLog.truncate(nextIndex);
                    raftLog.append(nextTermAtIndex, commandSource, commandSequence, commandTimeNanos,
                            appendRequestDecoder.buffer(),
                            offset,
                            length);
                    break;
                default:
                    throw new IllegalStateException("Invalid containment");
            }
            appendRequestDecoder.limit(offset + length);
        }
        return nextIndex;
    }

}
