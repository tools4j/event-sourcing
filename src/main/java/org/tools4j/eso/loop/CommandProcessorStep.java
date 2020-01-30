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
package org.tools4j.eso.loop;

import org.tools4j.eso.app.Application;
import org.tools4j.eso.cmd.AdminCommandProcessor;
import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.evt.FlyweightEventRouter;
import org.tools4j.eso.log.PeekableMessageLog;
import org.tools4j.eso.time.TimerControl;
import org.tools4j.nobark.loop.Step;

import static java.util.Objects.requireNonNull;
import static org.tools4j.eso.log.PeekableMessageLog.PeekPollHandler.Result.POLL;

public class CommandProcessorStep implements Step {

    private final PeekableMessageLog.PeekablePoller<? extends Command> commandPoller;
    private final FlyweightEventRouter eventRouter;
    private final TimerControl timerControl;
    private final AdminCommandProcessor adminCommandProcessor;
    private final Application application;
    private final PeekableMessageLog.PeekPollHandler<Command> handler = this::onCommand;

    public CommandProcessorStep(final PeekableMessageLog.PeekablePoller<? extends Command> commandPoller,
                                final FlyweightEventRouter eventRouter,
                                final TimerControl timerControl,
                                final AdminCommandProcessor adminCommandProcessor,
                                final Application application) {
        this.commandPoller = requireNonNull(commandPoller);
        this.eventRouter = requireNonNull(eventRouter);
        this.timerControl = requireNonNull(timerControl);
        this.adminCommandProcessor = requireNonNull(adminCommandProcessor);
        this.application = requireNonNull(application);
    }

    @Override
    public boolean perform() {
        return commandPoller.peekOrPoll(handler) > 0;
    }

    private PeekableMessageLog.PeekPollHandler.Result onCommand(final Command command) {
        try {
            eventRouter.start(command);
            adminCommandProcessor.onCommand(command, eventRouter, timerControl);
            application.commandProcessor().onCommand(command, eventRouter, timerControl);
            eventRouter.commit();//TODO add abort if there is an exception
        } catch (final Throwable t) {
            application.exceptionHandler().handleException(command, t);
        }
        return POLL;
    }
}
