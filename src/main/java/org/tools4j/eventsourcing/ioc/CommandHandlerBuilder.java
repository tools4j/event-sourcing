/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eventsourcing.ioc;

import org.tools4j.eventsourcing.command.AdminCommands;
import org.tools4j.eventsourcing.command.TimerCommands;
import org.tools4j.eventsourcing.core.*;

import java.util.Objects;

public class CommandHandlerBuilder {

    private final ServerBuilder serverBuilder;
    private final QueueBuilder queueBuilder;

    private DefaultCommandHandler defaultCommandHandler;
    private MultipartCommandHandler multipartCommandHandler;

    public CommandHandlerBuilder(final ServerBuilder serverBuilder,
                                 final QueueBuilder queueBuilder) {
        this.serverBuilder = Objects.requireNonNull(serverBuilder);
        this.queueBuilder = Objects.requireNonNull(queueBuilder);
    }

    DefaultCommandHandler defaultCommandHandler() {
        if (defaultCommandHandler == null) {
            final DefaultCommandHandler.SingleEventAppender singleEventAppender = new DefaultCommandHandler.SingleEventAppender(queueBuilder.outputAppender());
            final SingleCommitCommands singleCommitCommands = new SingleCommitCommands(singleEventAppender);
            final TimerCommands timerCommands = new DefaultTimerCommands(serverBuilder.timerIdProvider(), singleCommitCommands);
            final AdminCommands adminCommands = new DefaultAdminCommands(serverBuilder.serverConfig(), singleCommitCommands);
            defaultCommandHandler = new DefaultCommandHandler(singleEventAppender, timerCommands, adminCommands, singleCommitCommands, multipartCommandHandler());
        }
        return defaultCommandHandler;
    }

    MultipartCommandHandler multipartCommandHandler() {
        if (multipartCommandHandler == null) {
            final MultipartCommitCommands multipartCommitCommands = new MultipartCommitCommands();
            final TimerCommands timerCommands = new DefaultTimerCommands(serverBuilder.timerIdProvider(), multipartCommitCommands);
            final AdminCommands adminCommands = new DefaultAdminCommands(serverBuilder.serverConfig(), multipartCommitCommands);
            multipartCommandHandler = new MultipartCommandHandler(queueBuilder.outputAppender(), timerCommands, adminCommands, multipartCommitCommands);
        }
        return multipartCommandHandler;
    }

}
