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
package org.tools4j.eventsourcing.core;

import org.tools4j.eventsourcing.application.TimerService;
import org.tools4j.eventsourcing.event.DefinedHeaderEvent;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.TimerHeader;
import org.tools4j.eventsourcing.store.OutputQueue;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

public class EventAppendingTimerService implements TimerService {

    private final IntSupplier idProvider;
    private final OutputQueue.Appender appender;
    private final TimerHeader header = new TimerHeader();
    private final DefinedHeaderEvent event = new DefinedHeaderEvent(header);

    public EventAppendingTimerService(final IntSupplier idProvider,
                                      final OutputQueue.Appender appender) {
        this.idProvider = Objects.requireNonNull(idProvider);
        this.appender = Objects.requireNonNull(appender);
        this.header.version(Version.current());
    }

    @Override
    public int startTimer(final long timeout, final TimeUnit unit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout cannot be negative: " + timeout);
        }
        final int id = idProvider.getAsInt();
        header.start(id);
        switch (unit) {
            case NANOSECONDS:
            case MICROSECONDS:
                header.timeoutMicros(toMicros(timeout, unit));
                break;
            default:
                header.timeoutMillis(toMillis(timeout, unit));
                break;
        }
        appender.append(event);
        return id;
    }

    @Override
    public void stopTimer(final int timerId) {
        header.stop(timerId);
        header.timeoutMillis(0);
        appender.append(event);
    }

    //PRECONDITION: timeout >= 0
    private static int toMicros(final long timeout, final TimeUnit timeUnit) {
        final long micros = timeUnit.toMicros(timeout);
        return (int)Math.min(micros, Integer.MAX_VALUE);
    }

    //PRECONDITION: timeout >= 0
    private static int toMillis(final long timeout, final TimeUnit timeUnit) {
        final long micros = timeUnit.toMillis(timeout);
        return (int)Math.min(micros, Integer.MAX_VALUE);
    }
}
