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

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.tools4j.eso.cmd.AdminCommands;
import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.cmd.FlyweightCommand;
import org.tools4j.eso.log.MessageLog;
import org.tools4j.eso.src.SequenceGenerator;
import org.tools4j.eso.state.TimerState;
import org.tools4j.eso.time.TimeSource;
import org.tools4j.nobark.loop.Step;

import static java.util.Objects.requireNonNull;
import static org.tools4j.eso.cmd.AdminCommands.TIMER_PAYLOAD_SIZE;
import static org.tools4j.eso.cmd.FlyweightCommand.HEADER_OFFSET;

public final class TimerTriggerStep implements Step {

    private final MessageLog.Appender<? super Command> commandLogAppender;
    private final TimeSource timeSource;
    private final TimerState timerState;
    private final SequenceGenerator adminSequenceGenerator;

    private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(
            FlyweightCommand.HEADER_LENGTH + TIMER_PAYLOAD_SIZE);
    private final FlyweightCommand flyweightCommand = new FlyweightCommand();

    private long lastTime = Long.MIN_VALUE;

    public TimerTriggerStep(final MessageLog.Appender<? super Command> commandLogAppender,
                            final TimeSource timeSource,
                            final TimerState timerState,
                            final SequenceGenerator adminSequenceGenerator) {
        this.commandLogAppender = requireNonNull(commandLogAppender);
        this.timeSource = requireNonNull(timeSource);
        this.timerState = requireNonNull(timerState);
        this.adminSequenceGenerator = requireNonNull(adminSequenceGenerator);
    }

    @Override
    public boolean perform() {
        final int count = timerState.count();
        if (count == 0) {
            return false;
        }
        final long time = timeSource.currentTime();
        if (time < lastTime + timerState.tickTime()) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            final long timeout = timerState.timeout(i);
            if (timeout <= time) {
                trigger(time, timerState.type(i), timerState.id(i), timeout);
            }
        }
        lastTime = time;
        return true;
    }

    private void trigger(final long time, final int type, final long id, final long timeout) {
        final long sequence = adminSequenceGenerator.nextSequence();
        commandLogAppender.append(AdminCommands.triggerTimer(
                flyweightCommand, buffer, HEADER_OFFSET, sequence, time, type, id, timeout
        ));
        flyweightCommand.reset();
    }
}
