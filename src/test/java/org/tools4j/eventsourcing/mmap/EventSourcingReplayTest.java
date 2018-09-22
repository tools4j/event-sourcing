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
package org.tools4j.eventsourcing.mmap;

import org.agrona.collections.MutableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.api.EventProcessingQueue;
import org.tools4j.eventsourcing.api.EventProcessingState;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.common.BranchedIndexedTransactionalQueue;
import org.tools4j.mmap.region.api.RegionRingFactory;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public class EventSourcingReplayTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventSourcingReplayTest.class);

    public static void main(String... args) throws Exception {

        final RegionRingFactory regionRingFactory = TestUtil.getRegionRingFactory(args);

        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;
        final BooleanSupplier leadership = () -> true;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        final long replayFromSourceSeq = replayFromSourceSeq(args);
        final long replayToSourceSeq = replayToSourceSeq(args);

        final MutableReference<EventProcessingState> commitStateRef = new MutableReference<>();

        final EventProcessingQueue queue = EventProcessingQueue.builder()
                .upstreamQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("upstream")
                                .regionRingFactory(regionRingFactory)
                                .buildReadOnlyQueue())
                .downstreamQueue(
                        BranchedIndexedTransactionalQueue.builder()
                                .basePollerFactory(
                                        MmapBuilder.create()
                                                .directory(directory)
                                                .filePrefix("downstream")
                                                .regionRingFactory(regionRingFactory)
                                                .buildPollerFactory())
                                .branchQueue(
                                        MmapBuilder.create()
                                                .directory(directory)
                                                .filePrefix("downstream_branch")
                                                .regionRingFactory(regionRingFactory)
                                                .clearFiles(true)
                                                .buildTransactionalQueue())
                                .branchPredicate(
                                        (index, source, sourceSeq, eventTimeNanos) -> sourceSeq == replayFromSourceSeq)
                                .build())
                .upstreamFactory(
                        (downstreamAppender, upstreamBeforeState, downstreamAfterState) ->
                                (buffer, offset, length) -> {
                                    LOGGER.info("Replaying sourceSeq {}, already applied sourceSeq {}", upstreamBeforeState.sourceSeq(), downstreamAfterState.sourceSeq());
                                    downstreamAppender.accept(buffer, offset, length);
                                })
                .downstreamFactory(
                        (upstreamBeforeState, downstreamAfterState) -> {
                            commitStateRef.set(downstreamAfterState);
                            return stateMessageConsumer;
                        })
                .systemNanoClock(systemNanoClock)
                .leadership(leadership)
                .build();

        regionRingFactory.onComplete();

        TestUtil.startService("replay-processor",
                queue.processorStep(),
                () -> commitStateRef.get().sourceSeq() == replayToSourceSeq);
    }


    private static long replayFromSourceSeq(final String[] args) {
        return longArgument(args, "start sourceSeq", 1);
    }

    private static long replayToSourceSeq(final String[] args) {
        return longArgument(args, "end sourceSeq", 2);
    }

    private static long longArgument(final String[] args, final String name, final int position) {
        final String errorMessage = "Please specify " + name + " at position " + (position + 1) + ". It should be provided in the output of last perf test run";
        if (args.length < position + 1) {
            throw new IllegalArgumentException(errorMessage);
        }
        try {
            return Long.valueOf(args[position]);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}