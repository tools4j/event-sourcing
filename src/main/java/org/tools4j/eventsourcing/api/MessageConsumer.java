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

import org.agrona.DirectBuffer;

import java.util.Objects;

/**
 * Interface that allows to process a message in a buffer located at an offset that has a specified length.
 */
public interface MessageConsumer {
    MessageConsumer NO_OP = (buffer, offset, length) -> {};
    /**
     * Consumes a message.
     * @param buffer - direct buffer to read message from
     * @param offset - offset of the message in the buffer
     * @param length - length of the message
     */
    void accept(DirectBuffer buffer, int offset, int length);

    /**
     * Returns a composed {@code MessageConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code MessageConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default MessageConsumer andThen(final MessageConsumer after) {
        Objects.requireNonNull(after);
        return (b, o, l) -> {
            accept(b, o, l); after.accept(b, o, l);
        };
    }


    /**
     * Factory of a command executor. It should be implemented by the user of the library.
     * Command executor executes incoming commands and produces state-changing events that are to be applied to
     * EventApplier.
     */
    interface CommandExecutorFactory {
        CommandExecutorFactory PASS_THROUGH = (eventApplier,
                                               currentCommandExecutionState,
                                               completedCommandExecutionState,
                                               currentEventApplyingState,
                                               completedEventApplyingState) -> eventApplier;
        CommandExecutorFactory NO_OP = (eventApplier,
                                        currentCommandExecutionState,
                                        completedCommandExecutionState,
                                        currentEventApplyingState,
                                        completedEventApplyingState) -> MessageConsumer.NO_OP;
        /**
         * Constructs a new command executor.
         * @param eventApplier - resulting event consumer which applies the events to the application.
         * @param currentCommandExecutionState - current command execution state
         * @param completedCommandExecutionState - completed command execution state
         * @param currentEventApplyingState - current event applying state
         * @param completedEventApplyingState - completed event applying state
         * @return command executor.
         */
        MessageConsumer create(MessageConsumer eventApplier,
                               ProgressState currentCommandExecutionState,
                               ProgressState completedCommandExecutionState,
                               ProgressState currentEventApplyingState,
                               ProgressState completedEventApplyingState);
    }

    /**
     * Factory for event applier. It should be implemented by the user of the library.
     * Event Applier applies the state-changing events to the application state.
     * It should be free of side-effects.
     */
    interface EventApplierFactory {
        EventApplierFactory NO_OP = (currentEventApplyingState, completedEventApplyingState) -> MessageConsumer.NO_OP;
        /**
         * Constructs a new event applier.
         * @param currentEventApplyingState - current event applying state
         * @param completedEventApplyingState - completed event applying state
         * @return event applier
         */
        MessageConsumer create(ProgressState currentEventApplyingState,
                               ProgressState completedEventApplyingState);
    }
}
