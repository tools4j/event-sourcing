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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.tools4j.eventsourcing.mmap.MmapBuilder;
import org.tools4j.eventsourcing.mmap.RegionRingFactoryConfig;
import org.tools4j.mmap.region.api.RegionRingFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

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

    @Before
    public void setUp() throws Exception {
    }

    private void initQueue(final MessageConsumer.CommandExecutorFactory commandExecutorFactory) throws IOException {
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
                .stateChangingSource(value -> value < NON_STATE_CHANGING_SOURCE_LOW)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        commandExecutionQueue.close();
    }

    @Test
    public void nonStateChangingCommandShouldBeExecutedRegardless() throws Exception {
        initQueue((eventApplier, currentCommandExecutionState, completedEventApplierState) -> commandExecutor);

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(0, testMessage.getBytes());

        final int size = testMessage.getBytes().length;

        final int source = NON_STATE_CHANGING_SOURCE_LOW;
        final LongSupplier timeSupplier = System::nanoTime;


        //when
        commandExecutionQueue.appender().accept(source, 1, timeSupplier.getAsLong(), unsafeBuffer, 0, size);
        commandExecutionQueue.appender().accept(source, 2, timeSupplier.getAsLong(), unsafeBuffer, 0, size);

        commandExecutionQueue.executorStep().perform(); //initial apply events
        commandExecutionQueue.executorStep().perform(); //execute command 1
        commandExecutionQueue.executorStep().perform(); //attempt apply events produced by command 1 (none expected)
        commandExecutionQueue.executorStep().perform(); //execute command 2

        verify(commandExecutor, times(2)).accept(any(UnsafeBuffer.class), eq(12), eq(size));
    }


    @Test
    public void stateChangingCommandShouldBeExecuted() throws Exception {
        initQueue((eventApplier, currentCommandExecutionState, completedEventApplierState) -> commandExecutor);

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(0, testMessage.getBytes());

        final int size = testMessage.getBytes().length;

        final int source = NON_STATE_CHANGING_SOURCE_LOW - 1;
        final LongSupplier timeSupplier = System::nanoTime;


        //when
        commandExecutionQueue.appender().accept(source, 1, timeSupplier.getAsLong(), unsafeBuffer, 0, size);
        commandExecutionQueue.appender().accept(source, 2, timeSupplier.getAsLong(), unsafeBuffer, 0, size);

        commandExecutionQueue.executorStep().perform(); //initial apply events
        commandExecutionQueue.executorStep().perform(); //execute command 1
        commandExecutionQueue.executorStep().perform(); //apply events produced by command 1
        commandExecutionQueue.executorStep().perform(); //execute command 2

        verify(commandExecutor, times(2)).accept(any(UnsafeBuffer.class), eq(12), eq(size));
    }

}