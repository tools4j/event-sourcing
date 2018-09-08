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

import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.tools4j.eventsourcing.event.DefinedHeaderEvent;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.TimerHeader;
import org.tools4j.nobark.loop.Step;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class TimerController {

    private final TimerHeader header = new TimerHeader();
    private final LongSupplier nanoTimeSupplier;
    private final IntArrayList ids = new IntArrayList();
    private final LongArrayList startNanos = new LongArrayList();
    private final LongArrayList timeoutNanos = new LongArrayList();

    public TimerController() {
        this(System::nanoTime);
    }

    public TimerController(final LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier);
    }

    public int start(final int id, final long timeoutNs) {
        ids.addInt(id);
        startNanos.addLong(nanoTimeSupplier.getAsLong());
        timeoutNanos.addLong(timeoutNs);
        return id;
    }

    public boolean stop(final int timerId) {
        final int index = ids.indexOf(timerId);
        if (index >= 0) {
            remove(index);
            return true;
        }
        return false;
    }

    public Consumer<Event> startStopEventConsumer() {
        return event -> {
            if (event.type() == Type.TIMER) {
                header.init(event.header());
                if (header.isStarted()) {
                    final long timeoutNanos = (header.isMicros() ? TimeUnit.MICROSECONDS : TimeUnit.MILLISECONDS)
                            .toNanos(header.timeoutValue());
                    start(header.timerId(), timeoutNanos);
                } else if (header.isStopped()) {
                    stop(header.timerId());
                } else {
                    throw new IllegalArgumentException("Expected start or stop timer event: " + event);
                }
            }
        };
    }

    public Step expiryCheckerStep(final Consumer<? super Event> eventConsumer) {
        final TimerHeader header = new TimerHeader();
        header.version(Version.current());
        final DefinedHeaderEvent event = new DefinedHeaderEvent(header);
        return () -> {
            final int size = ids.size();
            if (size > 0) {
                final long time = nanoTimeSupplier.getAsLong();
                for (int i = 0; i < size; i++) {
                    if (time - startNanos.get(i) >= timeoutNanos.get(i)) {
                        final int id = remove(i);
                        header.eventTimeNanosSinceEpoch(time).expire(id);
                        eventConsumer.accept(event);
                        return true;
                    }
                }
            }
            return false;
        };
    }

    /**
     * Zero garbage insert order preserving removal
     * @param index the index of the timer to remove
     * @return the ID of the removed timer
     */
    private int remove(final int index) {
        final int last = ids.size() - 1;
        for (int i = index; i < last; i++) {
            ids.setInt(i, ids.getInt(i + 1));
            startNanos.setLong(i, startNanos.getLong(i + 1));
            timeoutNanos.setLong(i, timeoutNanos.getLong(i + 1));
        }
        final int id = ids.fastUnorderedRemove(last);
        startNanos.fastUnorderedRemove(last);
        timeoutNanos.fastUnorderedRemove(last);
        return id;
    }
}
