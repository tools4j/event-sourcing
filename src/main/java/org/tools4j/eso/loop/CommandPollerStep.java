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

import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.log.PeekableMessageLog;
import org.tools4j.nobark.loop.Step;

import static java.util.Objects.requireNonNull;

public class CommandPollerStep implements Step {

    private final PeekableMessageLog.PeekablePoller<? extends Command> commandPoller;
    private final PeekableMessageLog.PeekPollHandler<Command> handler;

    public CommandPollerStep(final PeekableMessageLog.PeekablePoller<? extends Command> commandPoller,
                             final PeekableMessageLog.PeekPollHandler<Command> handler) {
        this.commandPoller = requireNonNull(commandPoller);
        this.handler = requireNonNull(handler);
    }

    @Override
    public boolean perform() {
        return commandPoller.peekOrPoll(handler) > 0;
    }
}
