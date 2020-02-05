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
package org.tools4j.eso.command;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import org.tools4j.eso.log.MessageLog;
import org.tools4j.eso.input.SequenceGenerator;
import org.tools4j.eso.input.Input;
import org.tools4j.eso.time.TimeSource;

import static java.util.Objects.requireNonNull;
import static org.tools4j.eso.command.AdminCommands.TIMER_PAYLOAD_SIZE;

public class DefaultCommandLoopback implements CommandLoopback {

    private final MessageLog.Appender<? super Command> commandLogAppender;
    private final TimeSource timeSource;
    private final SequenceGenerator adminSequenceGenerator;

    private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(
            FlyweightCommand.HEADER_LENGTH + TIMER_PAYLOAD_SIZE);
    private final FlyweightCommand flyweightCommand = new FlyweightCommand();

    public DefaultCommandLoopback(final MessageLog.Appender<? super Command> commandLogAppender,
                                  final TimeSource timeSource,
                                  final SequenceGenerator adminSequenceGenerator) {
        this.commandLogAppender = requireNonNull(commandLogAppender);
        this.timeSource = requireNonNull(timeSource);
        this.adminSequenceGenerator = requireNonNull(adminSequenceGenerator);
    }

    @Override
    public void enqueueCommand(final int type, final DirectBuffer command, final int offset, final int length) {
        if (CommandType.isAdmin(type)) {
            throw new IllegalArgumentException("Invalid command type: " + type);
        }
        commandLogAppender.append(flyweightCommand.init(buffer, 0, Input.ADMIN_ID,
                adminSequenceGenerator.nextSequence(), type, timeSource.currentTime(), command, offset, length
        ));
        flyweightCommand.reset();
    }
}
