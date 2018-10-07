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
import org.tools4j.eventsourcing.api.CommandExecutionQueue;
import org.tools4j.eventsourcing.api.ProgressState;
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

        final MutableReference<ProgressState> commitStateRef = new MutableReference<>();

        final CommandExecutionQueue queue = CommandExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command")
                                .regionRingFactory(regionRingFactory)
                                .buildReadOnlyQueue())
                .eventQueue(
                        BranchedIndexedTransactionalQueue.builder()
                                .basePollerFactory(
                                        MmapBuilder.create()
                                                .directory(directory)
                                                .filePrefix("event")
                                                .regionRingFactory(regionRingFactory)
                                                .buildPollerFactory())
                                .branchQueue(
                                        MmapBuilder.create()
                                                .directory(directory)
                                                .filePrefix("event_branch")
                                                .regionRingFactory(regionRingFactory)
                                                .clearFiles(true)
                                                .buildTransactionalQueue())
                                .branchPredicate(
                                        (index, source, sourceSeq, eventTimeNanos) -> sourceSeq == replayFromSourceSeq)
                                .build())
                .commandExecutorFactory(
                        (eventApplier, currentCommandExecutionState, completedEventApplyingState) ->
                                (buffer, offset, length) -> {
                                    LOGGER.info("Replaying sourceSeq {}, already applied sourceSeq {}", currentCommandExecutionState.sourceSeq(), completedEventApplyingState.sourceSeq());
                                    eventApplier.accept(buffer, offset, length);
                                })
                .eventApplierFactory(
                        (currentEventApplyingState, completedEventApplyingState) -> {
                            commitStateRef.set(completedEventApplyingState);
                            return stateMessageConsumer;
                        })
                .systemNanoClock(systemNanoClock)
                .leadership(leadership)
                .build();

        regionRingFactory.onComplete();

        TestUtil.startService("replay-processor",
                queue.executorStep(),
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