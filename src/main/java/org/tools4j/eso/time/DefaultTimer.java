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
package org.tools4j.eso.time;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import org.tools4j.eso.evt.AdminEvents;
import org.tools4j.eso.evt.EventRouter;
import org.tools4j.eso.src.SequenceGenerator;
import org.tools4j.eso.state.Timers;

import static java.util.Objects.requireNonNull;

public class DefaultTimer implements Timer {

    private final SequenceGenerator timerIdGenerator;
    private final Timers timers;
    private final EventRouter eventRouter;

    private final MutableDirectBuffer payload = new ExpandableDirectByteBuffer(AdminEvents.TIMER_PAYLOAD_SIZE);

    public DefaultTimer(final SequenceGenerator timerIdGenerator,
                        final Timers timers,
                        final EventRouter eventRouter) {
        this.timerIdGenerator = requireNonNull(timerIdGenerator);
        this.timers = requireNonNull(timers);
        this.eventRouter = requireNonNull(eventRouter);
    }

    @Override
    public long startTimer(final int type, final long timeout) {
        final long id = timerIdGenerator.nextSequence();
        AdminEvents.timerStarted(payload, 0, type, id, timeout, eventRouter);
        return id;
    }

    @Override
    public boolean stopTimer(final long id) {
        final int count = timers.count();
        for (int i = 0; i < count; i++) {
            if (id == timers.id(i)) {
                AdminEvents.timerStopped(payload, 0, timers.type(i), id, timers.timeout(i), eventRouter);
                return true;
            }
        }
        return false;
    }
}
