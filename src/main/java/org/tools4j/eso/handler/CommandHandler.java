/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eso.handler;

import org.tools4j.eso.app.CommandProcessor;
import org.tools4j.eso.app.ExceptionHandler;
import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.evt.FlyweightEventRouter;
import org.tools4j.eso.log.PeekableMessageLog;

import static java.util.Objects.requireNonNull;
import static org.tools4j.eso.log.PeekableMessageLog.PeekPollHandler.Result.POLL;

public class CommandHandler implements PeekableMessageLog.PeekPollHandler<Command> {

    private final FlyweightEventRouter eventRouter;
    private final CommandProcessor commandProcessor;
    private final ExceptionHandler exceptionHandler;

    public CommandHandler(final FlyweightEventRouter eventRouter,
                          final CommandProcessor commandProcessor,
                          final ExceptionHandler exceptionHandler) {
        this.eventRouter = requireNonNull(eventRouter);
        this.commandProcessor = requireNonNull(commandProcessor);
        this.exceptionHandler = requireNonNull(exceptionHandler);
    }

    @Override
    public Result onMessage(final Command command) {
        try {
            eventRouter.start(command);
            commandProcessor.onCommand(command, eventRouter);
            eventRouter.commit();//TODO add abort if there is an exception
        } catch (final Throwable t) {
            exceptionHandler.handleException(command, t);
        }
        return POLL;
    }
}