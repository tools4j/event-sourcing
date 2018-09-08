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

import org.tools4j.eventsourcing.application.ApplicationHandler;
import org.tools4j.eventsourcing.application.CommandHandler;
import org.tools4j.eventsourcing.application.ServerContext;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.store.InputQueue;
import org.tools4j.eventsourcing.store.LongObjConsumer;
import org.tools4j.eventsourcing.store.OutputQueue;
import org.tools4j.nobark.loop.Step;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public class MainEventLoop implements Step {

    private final Step outputPollerStep;
    private final BooleanSupplier outputAppliedCondition;
    private final Step[] otherSteps;

    public MainEventLoop(final Step outputPollerStep,
                         final BooleanSupplier outputAppliedCondition,
                         final Step... otherSteps) {
        this.outputPollerStep = Objects.requireNonNull(outputPollerStep);
        this.outputAppliedCondition = Objects.requireNonNull(outputAppliedCondition);
        this.otherSteps = Objects.requireNonNull(otherSteps);
    }

    public static MainEventLoop create(final Step outputPollerStep,
                                       final BooleanSupplier outputAppliedCondition,
                                       final Step... otherSteps) {
        return new MainEventLoop(outputPollerStep, outputAppliedCondition, otherSteps);
    }

    public static MainEventLoop create(final InputQueue.Poller inputPoller,
                                       final OutputQueue.Poller outputPoller,
                                       final CommandHandler commandHandler,
                                       final ApplicationHandler applicationHandler,
                                       final ServerContext serverContext,
                                       final LongConsumer lastAppliedOutputIndexConsumer) {
        return create(
                outputPollerStep(outputPoller, applicationHandler, lastAppliedOutputIndexConsumer),
                outputAppliedCondition(serverContext),
                inputPollerStep(inputPoller, commandHandler, applicationHandler)
        );
    }

    @Override
    public boolean perform() {
        if (outputPollerStep.perform()) {
            return true;
        }
        if (outputAppliedCondition.getAsBoolean()) {
            boolean workDone = false;
            for (final Step step : otherSteps) {
                workDone |= step.perform();
            }
            return workDone;
        }
        return false;
    }

    public static Step outputPollerStep(final OutputQueue.Poller outputPoller,
                                        final ApplicationHandler applicationHandler,
                                        final LongConsumer lastAppliedOutputIndexConsumer) {
        Objects.requireNonNull(outputPoller);
        Objects.requireNonNull(applicationHandler);
        final LongObjConsumer<Event> eventConsumer = (index, event) -> {
            try {
                applicationHandler.applyOutputEvent(event);
                lastAppliedOutputIndexConsumer.accept(index);
            } catch (final Exception e) {
                //FIXME handle erors
                e.printStackTrace();
            }
        };
        return () -> outputPoller.poll(eventConsumer);
    }

    public static BooleanSupplier outputAppliedCondition(final ServerContext serverContext) {
        Objects.requireNonNull(serverContext);
        return () -> serverContext.lastAppendedOutputQueueIndex() == serverContext.lastAppliedOutputQueueIndex();
    }

    public static Step inputPollerStep(final InputQueue.Poller inputPoller,
                                       final CommandHandler commandHandler,
                                       final ApplicationHandler applicationHandler) {
        Objects.requireNonNull(inputPoller);
        Objects.requireNonNull(commandHandler);
        Objects.requireNonNull(applicationHandler);
        final Consumer<Event> eventConsumer = event -> {
            try {
                applicationHandler.processInputEvent(event, commandHandler);
            } catch (final Exception e) {
                //FIXME handle erors
                e.printStackTrace();
            }
        };
        return () -> inputPoller.poll(eventConsumer);
    }
}
