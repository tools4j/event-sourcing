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
package org.tools4j.eventsourcing.raft.mmap;

import io.aeron.Aeron;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.api.ExecutionQueue;
import org.tools4j.eventsourcing.api.IndexedAppender;
import org.tools4j.eventsourcing.api.IndexedPollerFactory;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.raft.state.*;
import org.tools4j.eventsourcing.raft.timer.Clock;
import org.tools4j.eventsourcing.raft.timer.DefaultTimer;
import org.tools4j.eventsourcing.raft.timer.Timer;
import org.tools4j.eventsourcing.raft.transport.LoggingPublisher;
import org.tools4j.eventsourcing.raft.transport.Poller;
import org.tools4j.eventsourcing.raft.transport.PollerFactory;
import org.tools4j.eventsourcing.raft.transport.Publisher;
import org.tools4j.eventsourcing.sbe.*;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.mmap.region.impl.MappedFile;
import org.tools4j.nobark.loop.Step;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.*;
import java.util.stream.IntStream;

public interface MmapRaftQueueBuilder {
    ServerIdStep clusterSize(int clusterSize);

    interface ServerIdStep {
        DirectoryStep serverId(int serverId);
    }

    interface DirectoryStep {
        FilePrefixStep directory(String directory);
    }

    interface FilePrefixStep {
        RegionRingFactoryStep filePrefix(String filePrefix);
    }

    interface RegionRingFactoryStep {
        Optionals regionRingFactory(RegionRingFactory regionRingFactory);
    }

    interface Optionals {
        Optionals clearFiles(boolean clearFiles);
        Optionals regionSize(int regionSize);
        Optionals regionRingSize(int regionRingSize);
        Optionals regionsToMapAhead(int regionsToMapAhead);
        Optionals maxFileSize(long maxFileSize);
        Optionals encodingBufferSize(int encodingBufferSize);

        Optionals onLeaderTransitionHandler(final IntConsumer onLeaderTransitionHandler);
        Optionals onFollowerTransitionHandler(final IntConsumer onFollowerTransitionHandler);
        Optionals minElectionTimeoutMillis(final int minElectionTimeoutMillis);
        Optionals maxElectionTimeoutMillis(final int maxElectionTimeoutMillis);
        Optionals heartbeatTimeoutMillis(final int heartbeatTimeoutMillis);
        Optionals maxMessagesPollable(final int maxMessagesPollable);
        Optionals maxAppendBatchSize(final int maxAppendBatchSize);
        Optionals clock(final Clock clock);
        Optionals logInMessages(boolean logInMessages);
        Optionals logOutMessages(boolean logOutMessages);
        Optionals truncateHandler(LongConsumer truncateHandler);

        ExecutionQueue build() throws IOException;
    }

    static MmapRaftQueueBuilder forAeronTransport(final Aeron aeron, final IntFunction<String> serverToChannel) {
        return new DefaultMmapRaftQueueBuilder(aeron, serverToChannel);
    }

    static MmapRaftQueueBuilder forCustomTransport(final IntFunction<? extends PollerFactory> serverToPollerFactory,
                                               final IntFunction<? extends Publisher> serverToPublisherFactory) {
        return new DefaultMmapRaftQueueBuilder(serverToPollerFactory, serverToPublisherFactory);
    }

    class DefaultMmapRaftQueueBuilder implements MmapRaftQueueBuilder, MmapRaftQueueBuilder.ServerIdStep, MmapRaftQueueBuilder.DirectoryStep, MmapRaftQueueBuilder.FilePrefixStep, MmapRaftQueueBuilder.RegionRingFactoryStep, MmapRaftQueueBuilder.Optionals {
        private final IntFunction<? extends PollerFactory> serverToPollerFactory;
        private final IntFunction<? extends Publisher> serverToPublisherFactory;

        private String directory;
        private String filePrefix;
        private RegionRingFactory regionRingFactory;
        private boolean clearFiles = false;
        private int regionSize = (int) MappedFile.REGION_SIZE_GRANULARITY * 1024 * 4;
        private int regionRingSize = 4;
        private int regionsToMapAhead = 1;
        private long maxFileSize = 1024 * 1024 * 1024 * 2L;
        private int encodingBufferSize = 8 * 1024;


        private IntConsumer onLeaderTransitionHandler = serverId -> {};
        private IntConsumer onFollowerTransitionHandler = serverId -> {};
        private int clusterSize;
        private int serverId;
        private int minElectionTimeoutMillis = 1100;
        private int maxElectionTimeoutMillis = 1500;
        private int heartbeatTimeoutMillis = 1000;
        private int maxMessagesPollable = 1;
        private int maxAppendBatchSize = 1;
        private Clock clock = Clock.DEFAULT;
        private boolean logInMessages = false;
        private boolean logOutMessages = false;
        private LongConsumer truncateHandler = size -> {};

        public DefaultMmapRaftQueueBuilder(final Aeron aeron,
                                           final IntFunction<String> serverToChannel) {
            this.serverToPollerFactory = PollerFactory.aeronServerToPollerFactory(aeron, serverToChannel);
            this.serverToPublisherFactory = serverId -> Publisher.aeronPublisher(aeron, serverToChannel.apply(serverId), serverId);
        }

        public DefaultMmapRaftQueueBuilder(final IntFunction<? extends PollerFactory> serverToPollerFactory,
                                           final IntFunction<? extends Publisher> serverToPublisherFactory) {

            this.serverToPollerFactory = serverToPollerFactory;
            this.serverToPublisherFactory = serverToPublisherFactory;
        }

        @Override
        public ServerIdStep clusterSize(final int clusterSize) {
            this.clusterSize = clusterSize;
            return this;
        }

        @Override
        public DirectoryStep serverId(final int serverId) {
            this.serverId = serverId;
            return this;
        }

        @Override
        public FilePrefixStep directory(final String directory) {
            this.directory = Objects.requireNonNull(directory);
            return this;
        }

        @Override
        public RegionRingFactoryStep filePrefix(final String filePrefix) {
            this.filePrefix = Objects.requireNonNull(filePrefix);
            return this;
        }

        @Override
        public Optionals regionRingFactory(final RegionRingFactory regionRingFactory) {
            this.regionRingFactory = Objects.requireNonNull(regionRingFactory);
            return this;
        }

        @Override
        public Optionals clearFiles(final boolean clearFiles) {
            this.clearFiles = clearFiles;
            return this;
        }

        @Override
        public Optionals regionSize(final int regionSize) {
            this.regionSize = regionSize;
            return this;
        }

        @Override
        public Optionals regionRingSize(final int regionRingSize) {
            this.regionRingSize = regionRingSize;
            return this;
        }

        @Override
        public Optionals regionsToMapAhead(final int regionsToMapAhead) {
            this.regionsToMapAhead = regionsToMapAhead;
            return this;
        }

        @Override
        public Optionals maxFileSize(final long maxFileSize) {
            this.maxFileSize = maxFileSize;
            return this;
        }

        @Override
        public Optionals encodingBufferSize(final int encodingBufferSize) {
            this.encodingBufferSize = encodingBufferSize;
            return this;
        }

        @Override
        public Optionals onLeaderTransitionHandler(final IntConsumer onLeaderTransitionHandler) {
            this.onLeaderTransitionHandler = Objects.requireNonNull(onLeaderTransitionHandler);
            return this;
        }

        @Override
        public Optionals onFollowerTransitionHandler(final IntConsumer onFollowerTransitionHandler) {
            this.onFollowerTransitionHandler = Objects.requireNonNull(onFollowerTransitionHandler);
            return this;
        }

        @Override
        public Optionals minElectionTimeoutMillis(final int minElectionTimeoutMillis) {
            this.minElectionTimeoutMillis = minElectionTimeoutMillis;
            return this;
        }

        @Override
        public Optionals maxElectionTimeoutMillis(final int maxElectionTimeoutMillis) {
            this.maxElectionTimeoutMillis = maxElectionTimeoutMillis;
            return this;
        }

        @Override
        public Optionals heartbeatTimeoutMillis(final int heartbeatTimeoutMillis) {
            this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
            return this;
        }

        @Override
        public Optionals maxMessagesPollable(final int maxMessagesPollable) {
            this.maxMessagesPollable = maxMessagesPollable;
            return this;
        }

        @Override
        public Optionals maxAppendBatchSize(final int maxAppendBatchSize) {
            this.maxAppendBatchSize = maxAppendBatchSize;
            return this;
        }


        @Override
        public Optionals clock(final Clock clock) {
            this.clock = Objects.requireNonNull(clock);
            return this;
        }


        @Override
        public Optionals logInMessages(final boolean logInMessages) {
            this.logInMessages = logInMessages;
            return this;
        }

        @Override
        public Optionals logOutMessages(final boolean logOutMessages) {
            this.logOutMessages = logOutMessages;
            return this;
        }

        @Override
        public Optionals truncateHandler(final LongConsumer truncateHandler) {
            this.truncateHandler = Objects.requireNonNull(truncateHandler);
            return this;
        }

        @Override
        public ExecutionQueue build() throws IOException {
            Objects.requireNonNull(directory);

            if (serverId < 0 || serverId >= clusterSize) {
                throw new IllegalArgumentException("Invalid serverId. Must be value [0..clusterSize)");
            }
            final Logger outLogger = LoggerFactory.getLogger("OUT");
            final Logger inLogger = LoggerFactory.getLogger("IN");
            final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
            final AppendRequestEncoder appendRequestEncoder = new AppendRequestEncoder();
            final AppendResponseEncoder appendResponseEncoder = new AppendResponseEncoder();
            final VoteResponseEncoder voteResponseEncoder = new VoteResponseEncoder();
            final VoteRequestEncoder voteRequestEncoder = new VoteRequestEncoder();

            final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
            final VoteRequestDecoder voteRequestDecoder = new VoteRequestDecoder();
            final VoteResponseDecoder voteResponseDecoder = new VoteResponseDecoder();
            final AppendRequestDecoder appendRequestDecoder = new AppendRequestDecoder();
            final AppendResponseDecoder appendResponseDecoder = new AppendResponseDecoder();

            final UnsafeBuffer encodingBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodingBufferSize));
            final UnsafeBuffer decodingBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodingBufferSize));

            final Publisher publisher = applyLoggingIfRequired(
                    serverToPublisherFactory.apply(serverId),
                    outLogger,
                    messageHeaderDecoder,
                    voteRequestDecoder,
                    voteResponseDecoder,
                    appendRequestDecoder,
                    appendResponseDecoder);

            final RaftLog raftLog = new MmapRaftLog(
                    RaftRegionAccessorSupplier.forReadWrite(
                            directory,
                            filePrefix+ "_" + serverId,
                            clearFiles,
                            regionRingFactory,
                            regionSize,
                            regionRingSize,
                            regionsToMapAhead,
                            maxFileSize),
                    truncateHandler);

            final IndexedPollerFactory committedLogPollerFactory = options ->
                    new MmapRaftPoller(RaftRegionAccessorSupplier.forReadOnly(
                            directory,
                            filePrefix+ "_" + serverId,
                            regionRingFactory,
                            regionSize,
                            regionRingSize,
                            regionsToMapAhead),
                            org.tools4j.eventsourcing.api.Poller.Options.builder()
                                    .skipWhen(options.skipWhen().and(
                                            (index, source, sourceSeq, eventTimeNanos) -> index <= raftLog.commitIndex()))
                                    .pauseWhen(options.pauseWhen().or(
                                            (index, source, sourceSeq, eventTimeNanos) ->  index > raftLog.commitIndex()))
                                    .resetWhen(options.resetWhen())
                                    .onProcessingStart(options.onProcessingStart())
                                    .onProcessingComplete(options.onProcessingComplete())
                                    .onProcessingSkipped(options.onProcessingSkipped())
                                    .onReset(options.onReset())
                                    .bufferPoller(options.bufferPoller())
                                    .build());

            final Supplier<Timer> heartbeatTimerFactory = () -> new DefaultTimer(clock, heartbeatTimeoutMillis, heartbeatTimeoutMillis);

            final Peers peers = new DefaultPeers(serverId, clusterSize, peerId -> new DefaultPeer(peerId, heartbeatTimerFactory.get()));

            final Timer electionTimer = new DefaultTimer(clock, minElectionTimeoutMillis, maxElectionTimeoutMillis);

            final AppendRequestHandler appendRequestHandler = new AppendRequestHandler(
                    raftLog,
                    electionTimer, messageHeaderEncoder,
                    appendResponseEncoder,
                    encodingBuffer,
                    publisher,
                    serverId);

            final VoteRequestHandler voteRequestHandler = new VoteRequestHandler(raftLog,
                    electionTimer, messageHeaderEncoder,
                    voteResponseEncoder,
                    encodingBuffer,
                    publisher,
                    serverId);

            final Predicate<HeaderDecoder> destinationFilter = DestinationFilter.forServer(serverId);

            final ServerState followerServerState = new HeaderFilteringServerState(destinationFilter,
                    applyLoggingIfRequired(
                            new HighTermHandlingServerState(
                                    new FollowerServerState(
                                            serverId,
                                            appendRequestHandler,
                                            voteRequestHandler,
                                            electionTimer,
                                            onFollowerTransitionHandler),
                                    raftLog, inLogger),
                            inLogger
                    ));

            final ServerState candidateServerState = new HeaderFilteringServerState(destinationFilter,
                    applyLoggingIfRequired(
                            new HighTermHandlingServerState(
                                    new CandidateServerState(raftLog,
                                            peers,
                                            appendRequestHandler,
                                            electionTimer,
                                            serverId,
                                            messageHeaderEncoder,
                                            voteRequestEncoder,
                                            encodingBuffer,
                                            publisher),
                                    raftLog, inLogger),
                            inLogger
                    ));

            final ServerState leaderServerState = new HeaderFilteringServerState(destinationFilter,
                    applyLoggingIfRequired(
                            new HighTermHandlingServerState(
                                    new LeaderServerState(raftLog,
                                            peers,
                                            serverId,
                                            appendRequestEncoder,
                                            messageHeaderEncoder,
                                            encodingBuffer,
                                            decodingBuffer,
                                            publisher,
                                            onLeaderTransitionHandler,
                                            maxAppendBatchSize),
                                    raftLog, inLogger),
                            inLogger
                    ));

            final ServerMessageHandler serverMessageHandler = new ServerMessageHandler(messageHeaderDecoder,
                    voteRequestDecoder,
                    voteResponseDecoder,
                    appendRequestDecoder,
                    appendResponseDecoder,
                    candidateServerState,
                    leaderServerState,
                    followerServerState,
                    followerServerState);

            final List<Step> processSteps = new ArrayList<>(clusterSize - 1 + 3);

            IntStream.range(0, clusterSize)
                    .filter(destinationId -> destinationId != serverId)
                    .forEach(destinationId -> {
                        final Poller destinationPoller = serverToPollerFactory.apply(destinationId)
                                .create(serverMessageHandler, maxMessagesPollable);
                        processSteps.add(destinationPoller::poll);
                    });

            processSteps.add(serverMessageHandler);

            final IndexedAppender raftQueueAppender = new IndexedAppender() {
                @Override
                public void accept(final int source, final long sourceSeq, final long eventTimeNanos, final DirectBuffer buffer, final int offset, final int length) {
                    serverMessageHandler.accept(source, sourceSeq, eventTimeNanos, buffer, offset, length);
                }

                @Override
                public long lastSourceSeq(final int source) {
                    return raftLog.lastSourceSeq(source);
                }
            };


            return new ExecutionQueue() {
                @Override
                public Step executorStep() {
                    return () -> {
                        boolean done = false;
                        for (int i = 0; i < processSteps.size(); i++) {
                            final Step step = processSteps.get(i);
                            done |= step.perform();
                        }
                        return done;
                    };
                }

                @Override
                public void init() {
                    serverMessageHandler.init();
                }

                @Override
                public IndexedAppender appender() {
                    return raftQueueAppender;
                }

                @Override
                public org.tools4j.eventsourcing.api.Poller createPoller(final org.tools4j.eventsourcing.api.Poller.Options options) throws IOException {
                    return committedLogPollerFactory.createPoller(options);
                }

                @Override
                public boolean leader() {
                    return serverMessageHandler.leader();
                }

                @Override
                public void close() {
                    raftLog.close();
                }
            };

        }

        private ServerState applyLoggingIfRequired(final ServerState serverState, final Logger logger) {
            return logInMessages ? new LoggingServerState(serverState, new StringBuilder(), logger) : serverState;
        }

        private Publisher applyLoggingIfRequired(final Publisher publisher,
                                                 final Logger logger,
                                                 final MessageHeaderDecoder messageHeaderDecoder,
                                                 final VoteRequestDecoder voteRequestDecoder,
                                                 final VoteResponseDecoder voteResponseDecoder,
                                                 final AppendRequestDecoder appendRequestDecoder,
                                                 final AppendResponseDecoder appendResponseDecoder) {
            return logOutMessages ? new LoggingPublisher(publisher, logger,
                    messageHeaderDecoder, voteRequestDecoder, voteResponseDecoder, appendRequestDecoder,
                    appendResponseDecoder, new StringBuilder())
                    : publisher;
        }
    }
}
