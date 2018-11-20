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
package org.tools4j.eventsourcing.api;


import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.tools4j.eventsourcing.TestMessage;
import org.tools4j.eventsourcing.mmap.MmapBuilder;
import org.tools4j.eventsourcing.mmap.RegionRingFactoryConfig;
import org.tools4j.mmap.region.api.RegionRingFactory;

import java.io.IOException;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ExecutionQueueTest {
    private static final int PAYLOAD_OFFSET = 0;
    @Mock
    private MessageConsumer commandExecutor;

    private ExecutionQueue executionQueue;

    private ProgressState compProgressState;

    private void initExecutionQueue(final CommandExecutorFactory commandExecutorFactory) throws IOException {
        final RegionRingFactory regionRingFactory = RegionRingFactoryConfig.get("SYNC");

        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        executionQueue = ExecutionQueue.builder()
                .commandQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("command")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .eventQueue(
                        MmapBuilder.create()
                                .directory(directory)
                                .filePrefix("event")
                                .regionRingFactory(regionRingFactory)
                                .clearFiles(true)
                                .buildQueue())
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentProgressState, completedProgressState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        executionQueue.close();
    }

    @Test
    public void command_should_be_executed() throws Exception {
        initExecutionQueue((eventApplier,
                            currentProgressState,
                            completedProgressState) -> commandExecutor); //no events are applied to eventApplier

        final TestMessage message = TestMessage.forDefaultLength();

        final int source = 100;
        final LongSupplier timeSupplier = System::nanoTime;

        //when
        executionQueue.appender().accept(source, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        executionQueue.appender().accept(source, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);

        executionQueue.executorStep().perform(); //execute command 1
        executionQueue.executorStep().perform(); //execute command 2

        //then
        verify(commandExecutor, times(2)).accept(any(UnsafeBuffer.class), eq(PAYLOAD_OFFSET), eq(message.length));
    }

    @Test
    public void nonStateChangingCommands_state_should_be_caught_up_when_stateChangingCommand_is_applied() throws Exception {
        initExecutionQueue((eventApplier,
                            currentProgressState,
                            completedProgressState) -> {
            compProgressState = completedProgressState;
            return MessageConsumer.NO_OP;
        }); //no events are applied to eventApplier

        final TestMessage message = TestMessage.forDefaultLength();

        final int source0 = 10;
        final int source1 = 20;
        final int source2 = 30;
        final LongSupplier timeSupplier = System::nanoTime;

        //when
        executionQueue.appender().accept(source1, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        executionQueue.appender().accept(source1, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        executionQueue.appender().accept(source2, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        executionQueue.appender().accept(source2, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        executionQueue.appender().accept(source0, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);

        //when
        executionQueue.executorStep().perform(); //execute command 1

        //then
        assertThat(compProgressState.sourceSeq(source1)).isEqualTo(1);

        //when
        executionQueue.executorStep().perform(); //execute command 2

        //then
        assertThat(compProgressState.sourceSeq(source1)).isEqualTo(2);

        //when
        executionQueue.executorStep().perform(); //execute command 3

        //then
        assertThat(compProgressState.sourceSeq(source2)).isEqualTo(1);

        //when
        executionQueue.executorStep().perform(); //execute command 4

        //then
        assertThat(compProgressState.sourceSeq(source2)).isEqualTo(2);

        //when
        executionQueue.executorStep().perform(); //execute command 5

        //then
        assertThat(compProgressState.sourceSeq(source0)).isEqualTo(1);
        assertThat(compProgressState.sourceSeq(source1)).isEqualTo(2);
        assertThat(compProgressState.sourceSeq(source2)).isEqualTo(2);
    }

}