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
package org.tools4j.eventsourcing.application;

import java.util.Objects;
import java.util.function.Consumer;

import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.store.InputQueue;
import org.tools4j.eventsourcing.store.OutputQueue;
import org.tools4j.eventsourcing.store.PollResut;
import org.tools4j.nobark.loop.Step;

public class MainEventLoop implements Step {

    private final InputQueue.Poller inputPoller;
    private final OutputQueue.Poller outputPoller;

    private final Consumer<? super Event> inputEventConsumer;
    private final Consumer<? super Event> outputEventConsumer;

    public MainEventLoop(final InputQueue.Poller inputPoller,
                         final OutputQueue.Poller outputPoller,
                         final CommandHandler commandHandler,
                         final ApplicationHandler applicationHandler) {
        this.inputPoller = Objects.requireNonNull(inputPoller);
        this.outputPoller = Objects.requireNonNull(outputPoller);
        Objects.requireNonNull(commandHandler);
        this.inputEventConsumer = event -> {
            try {
                applicationHandler.processInputEvent(event, commandHandler);
            } catch (final Exception e) {
                //FIXME handle erors
                e.printStackTrace();
            }
        };
        this.outputEventConsumer = event -> {
            try {
                applicationHandler.applyOutputEvent(event);
            } catch (final Exception e) {
                //FIXME handle erors
                e.printStackTrace();
            }
        };
    }

    @Override
    public boolean perform() {
        final PollResut outputPollResult;
        if ((outputPollResult = outputPoller.poll(outputEventConsumer)) == PollResut.POLLED) {
            return true;
        }
        if (outputPollResult == PollResut.END) {
            if (inputPoller.poll(inputEventConsumer)) {
                return true;
            }
        }
        return false;
    }

}
