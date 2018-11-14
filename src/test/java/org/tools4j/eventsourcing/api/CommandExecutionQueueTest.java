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
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CommandExecutionQueueTest {
    private static final int NON_STATE_CHANGING_SOURCE_LOW = 10000000;
    @Mock
    private MessageConsumer commandExecutor;

    private CommandExecutionQueue commandExecutionQueue;

    private ProgressState curCommandExecutionState;
    private ProgressState compCommandExecutionState;
    private ProgressState curEventApplyingState;
    private ProgressState compEventApplierState;

    private void initExecutionQueue(final MessageConsumer.CommandExecutorFactory commandExecutorFactory) throws IOException {
        final RegionRingFactory regionRingFactory = RegionRingFactoryConfig.get("SYNC");

        final String directory = System.getProperty("user.dir") + "/build";
        final LongSupplier systemNanoClock = System::nanoTime;
        final BooleanSupplier leadership = () -> true;

        final MessageConsumer stateMessageConsumer = (buffer, offset, length) -> {};

        commandExecutionQueue = CommandExecutionQueue.builder()
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
                                .buildTransactionalQueue())
                .commandExecutorFactory(commandExecutorFactory)
                .eventApplierFactory(
                        (currentEventApplierState, completedEventApplierState) -> stateMessageConsumer)
                .systemNanoClock(systemNanoClock)
                .leadership(leadership)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        commandExecutionQueue.close();
    }

    @Test
    public void nonStateChangingCommand_should_be_executed_when_no_events_are_produced() throws Exception {
        initExecutionQueue((eventApplier,
                            currentCommandExecutionState,
                            completedCommandExecutionState,
                            currentEventApplyingState,
                            completedEventApplierState) -> commandExecutor); //no events are applied to eventApplier

        final TestMessage message = TestMessage.forDefaultLength();

        final int source = NON_STATE_CHANGING_SOURCE_LOW;
        final LongSupplier timeSupplier = System::nanoTime;

        //when
        commandExecutionQueue.appender().accept(source, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        commandExecutionQueue.appender().accept(source, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);

        commandExecutionQueue.executorStep().perform(); //initial apply events
        commandExecutionQueue.executorStep().perform(); //execute command 1
        commandExecutionQueue.executorStep().perform(); //attempt apply events produced by command 1 (none expected)
        commandExecutionQueue.executorStep().perform(); //execute command 2

        //then
        verify(commandExecutor, times(2)).accept(any(UnsafeBuffer.class), eq(12), eq(message.length));
    }


    @Test
    public void stateChangingCommand_should_be_executed_when_previous_command_produced_no_events() throws Exception {
        initExecutionQueue((eventApplier,
                            currentCommandExecutionState,
                            completedCommandExecutionState,
                            currentEventApplyingState,
                            completedEventApplierState) -> commandExecutor); //no events are applied to eventApplier

        final TestMessage message = TestMessage.forDefaultLength();

        final int source = NON_STATE_CHANGING_SOURCE_LOW - 1;
        final LongSupplier timeSupplier = System::nanoTime;


        //when
        commandExecutionQueue.appender().accept(source, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        commandExecutionQueue.appender().accept(source, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);

        commandExecutionQueue.executorStep().perform(); //initial apply events
        commandExecutionQueue.executorStep().perform(); //execute command 1
        commandExecutionQueue.executorStep().perform(); //apply events produced by command 1 (non expected)
        commandExecutionQueue.executorStep().perform(); //execute command 2

        //then
        verify(commandExecutor, times(2)).accept(any(UnsafeBuffer.class), eq(12), eq(message.length));
    }


    //@Test
    public void nonStateChangingCommands_state_should_be_caught_up_when_stateChangingCommand_is_applied() throws Exception {
        initExecutionQueue((eventApplier,
                            currentCommandExecutionState,
                            completedCommandExecutionState,
                            currentEventApplyingState,
                            completedEventApplierState) -> {
            curCommandExecutionState = currentCommandExecutionState;
            compCommandExecutionState = completedCommandExecutionState;
            curEventApplyingState = currentEventApplyingState;
            compEventApplierState = completedEventApplierState;

            return commandExecutor;
        }); //no events are applied to eventApplier

        final TestMessage message = TestMessage.forDefaultLength();

        final int stateChangingSource = NON_STATE_CHANGING_SOURCE_LOW - 1;
        final int nonStateChangingSource1 = NON_STATE_CHANGING_SOURCE_LOW;
        final int nonStateChangingSource2 = NON_STATE_CHANGING_SOURCE_LOW + 1;
        final LongSupplier timeSupplier = System::nanoTime;

        //when
        commandExecutionQueue.appender().accept(nonStateChangingSource1, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        commandExecutionQueue.appender().accept(nonStateChangingSource1, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        commandExecutionQueue.appender().accept(nonStateChangingSource2, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        commandExecutionQueue.appender().accept(nonStateChangingSource2, 2, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);
        commandExecutionQueue.appender().accept(stateChangingSource, 1, timeSupplier.getAsLong(), message.buffer, message.offset, message.length);

        commandExecutionQueue.executorStep().perform(); //initial apply events

        //when
        commandExecutionQueue.executorStep().perform(); //execute command 1
        commandExecutionQueue.executorStep().perform(); //attempt apply events produced by command 1 (none expected)

        //then
        assertThat(compEventApplierState.sourceSeq(nonStateChangingSource1)).isEqualTo(-1);

        //when
        commandExecutionQueue.executorStep().perform(); //execute command 2
        commandExecutionQueue.executorStep().perform(); //attempt apply events produced by command 2 (none expected)

        //then
        assertThat(compEventApplierState.sourceSeq(nonStateChangingSource1)).isEqualTo(-1);

        //when
        commandExecutionQueue.executorStep().perform(); //execute command 3
        commandExecutionQueue.executorStep().perform(); //attempt apply events produced by command 3 (none expected)

        //then
        assertThat(compEventApplierState.sourceSeq(nonStateChangingSource2)).isEqualTo(-1);

        //when
        commandExecutionQueue.executorStep().perform(); //execute command 4
        commandExecutionQueue.executorStep().perform(); //attempt apply events produced by command 4 (none expected)

        //then
        assertThat(compEventApplierState.sourceSeq(nonStateChangingSource2)).isEqualTo(-1);

        //when
        commandExecutionQueue.executorStep().perform(); //execute command 5
        commandExecutionQueue.executorStep().perform(); //attempt to apply events produced by command 5 (should be skipped as events of command 5 have been applied during command execution)
        commandExecutionQueue.executorStep().perform(); //attempt to execute a command
        commandExecutionQueue.executorStep().perform(); //catch up progress state for nonStateChangingSource1
        commandExecutionQueue.executorStep().perform(); //catch up progress state for nonStateChangingSource2

        //then
        assertThat(compEventApplierState.sourceSeq(stateChangingSource)).isEqualTo(1);
        assertThat(compEventApplierState.sourceSeq(nonStateChangingSource1)).isEqualTo(2);
        assertThat(compEventApplierState.sourceSeq(nonStateChangingSource2)).isEqualTo(2);

    }

}