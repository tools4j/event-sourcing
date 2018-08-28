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

import org.tools4j.nobark.loop.Step;

import java.util.Objects;

public class MainEventLoop implements Step {

    private final Queue.Poller inputPoller;
    private final Queue.Poller outputPoller;

    private final SingleEventAppender singleEventAppender;
    private final Queue.EventConsuer inputEventConsumer;
    private final Queue.EventConsuer outputEventConsumer;

    public MainEventLoop(final Queue.Poller inputPoller,
                         final Queue.Poller outputPoller,
                         final Queue.Appender outputAppender,
                         final ApplicationHandler applicationHandler) {
        this.inputPoller = Objects.requireNonNull(inputPoller);
        this.outputPoller = Objects.requireNonNull(outputPoller);
        this.singleEventAppender = new SingleEventAppender(applicationHandler, outputAppender);
        this.inputEventConsumer = singleEventAppender.inputEventHandler();
        this.outputEventConsumer = (queueIndex, event) -> {
            applicationHandler.applyOutputEvent(queueIndex, event);
            //FIXME update processed output state
        };
    }

    @Override
    public boolean perform() {
        final Queue.PollResut outputPollResult;
        if ((outputPollResult = outputPoller.poll(outputEventConsumer)) == Queue.PollResut.POLLED) {
            return true;
        }
        if (outputPollResult == Queue.PollResut.END) {
            if (inputPoller.poll(inputEventConsumer) == Queue.PollResut.POLLED) {
                return true;
            }
        }
        return false;
    }

}
