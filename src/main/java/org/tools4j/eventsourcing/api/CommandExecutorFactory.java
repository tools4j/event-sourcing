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

/**
 * Factory of a command executor. It should be implemented by the user of the library.
 * Command executor executes incoming commands and produces state-changing events that are to be applied to
 * EventApplier.
 */
public interface CommandExecutorFactory {
    CommandExecutorFactory PASS_THROUGH = (eventApplier,
                                           currentProgressState,
                                           completedProgressState) -> eventApplier;
    CommandExecutorFactory NO_OP = (eventApplier,
                                    currentProgressState,
                                    completedProgressState) -> MessageConsumer.NO_OP;
    /**
     * Constructs a new command executor.
     * @param eventApplier - resulting event consumer which applies the events to the application.
     * @param currentProgressState - current progress state
     * @param completedProgressState - completed progress state
     * @return command executor.
     */
    MessageConsumer create(MessageConsumer eventApplier,
                           ProgressState currentProgressState,
                           ProgressState completedProgressState);
}
