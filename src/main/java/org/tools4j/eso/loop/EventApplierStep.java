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
import org.tools4j.eso.cmd.CommandLoopback;
import org.tools4j.eso.evt.Event;
import org.tools4j.eso.log.MessageLog;
import org.tools4j.nobark.loop.Step;

import static java.util.Objects.requireNonNull;

public class EventApplierStep implements Step {

    private final MessageLog.Poller<? extends Event> eventPoller;
    private final CommandLoopback commandLoopback;
    private final Application application;
    private final MessageLog.Handler<Event> handler = this::onEvent;

    public EventApplierStep(final MessageLog.Poller<? extends Event> eventPoller,
                            final CommandLoopback commandLoopback,
                            final Application application) {
        this.eventPoller = requireNonNull(eventPoller);
        this.commandLoopback = requireNonNull(commandLoopback);
        this.application = requireNonNull(application);
    }

    @Override
    public boolean perform() {
        return eventPoller.poll(handler) > 0;
    }

    private void onEvent(final Event event) {
        try {
            application.eventApplier().onEvent(event, commandLoopback);
        } catch (final Throwable t) {
            application.exceptionHandler().handleException(event, t);
        }
    }
}
