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
package org.tools4j.eso.src;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.tools4j.eso.cmd.CommandType;
import org.tools4j.eso.state.TimerState;
import org.tools4j.eso.time.TimeSource;

import static java.util.Objects.requireNonNull;
import static org.tools4j.eso.cmd.AdminCommands.TIMER_PAYLOAD_SIZE;
import static org.tools4j.eso.cmd.AdminCommands.triggerTimer;

public final class TimerTriggerSource implements Source {

    private final TimeSource timeSource;
    private final TimerState timerState;
    private final SequenceGenerator adminSequenceGenerator;

    private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(TIMER_PAYLOAD_SIZE);
    private final Poller poller = new TimerTriggerPoller();

    public TimerTriggerSource(final TimeSource timeSource,
                              final TimerState timerState,
                              final SequenceGenerator adminSequenceGenerator) {
        this.timeSource = requireNonNull(timeSource);
        this.timerState = requireNonNull(timerState);
        this.adminSequenceGenerator = requireNonNull(adminSequenceGenerator);
    }

    @Override
    public int id() {
        return ADMIN_ID;
    }

    @Override
    public Poller poller() {
        return poller;
    }

    private class TimerTriggerPoller implements Poller {
        @Override
        public int poll(final Handler handler) {
            final int count = timerState.count();
            if (count == 0) {
                return 0;
            }
            final long time = timeSource.currentTime();
            int triggered = 0;
            for (int i = 0; i < count; i++) {
                final long timeout = timerState.timeout(i);
                if (timeout <= time) {
                    final long sequence = adminSequenceGenerator.nextSequence();
                    final int len = triggerTimer(buffer, 0, timerState.type(i), timerState.id(i), timeout);
                    handler.onMessage(sequence, CommandType.TRIGGER_TIMER.value(), buffer, 0, len);
                    triggered++;
                }
            }
            return triggered;
        }
    }
}
