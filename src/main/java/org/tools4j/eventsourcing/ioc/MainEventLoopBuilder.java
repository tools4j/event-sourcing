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
package org.tools4j.eventsourcing.ioc;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.tools4j.eventsourcing.application.ApplicationHandler;
import org.tools4j.eventsourcing.application.ServerContext;
import org.tools4j.eventsourcing.core.CommandController;
import org.tools4j.eventsourcing.core.MainEventLoop;
import org.tools4j.eventsourcing.event.Event;
import org.tools4j.eventsourcing.store.InputQueue;
import org.tools4j.eventsourcing.store.LongObjConsumer;
import org.tools4j.eventsourcing.store.OutputQueue;
import org.tools4j.nobark.loop.*;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public class MainEventLoopBuilder {

    private final QueueBuilder queueBuilder;
    private final CommandHandlerBuilder commandHandlerBuilder;
    private final ApplicationBuilder applicationBuilder;
    private final ServerBuilder serverBuilder;
    private final AdminControllerBuilder adminControllerBuilder;
    private final TimerControllerBuilder timerControllerBuilder;

    private MainEventLoop mainEventLoop;

    public MainEventLoopBuilder(final QueueBuilder queueBuilder,
                                final CommandHandlerBuilder commandHandlerBuilder,
                                final ApplicationBuilder applicationBuilder,
                                final ServerBuilder serverBuilder,
                                final AdminControllerBuilder adminControllerBuilder,
                                final TimerControllerBuilder timerControllerBuilder) {
        this.queueBuilder = Objects.requireNonNull(queueBuilder);
        this.commandHandlerBuilder = Objects.requireNonNull(commandHandlerBuilder);
        this.applicationBuilder = Objects.requireNonNull(applicationBuilder);
        this.serverBuilder = Objects.requireNonNull(serverBuilder);
        this.adminControllerBuilder = Objects.requireNonNull(adminControllerBuilder);
        this.timerControllerBuilder = Objects.requireNonNull(timerControllerBuilder);
    }

    public IdleStrategy idleStrategy() {
        final BackoffIdleStrategy boStrategy = new BackoffIdleStrategy(
                100, 10, 128, 1<<14
        );
        return new IdleStrategy() {
            @Override
            public void idle() {
                boStrategy.idle();
            }

            @Override
            public void reset() {
                boStrategy.reset();
            }

            @Override
            public String toString() {
                return boStrategy.toString();
            }
        };
    }

    public ExceptionHandler exceptionHandler() {
        return (loop, step, throwable) -> {
            System.err.println("Uncaught exception in step '" + step + ": e=" + throwable);
            throwable.printStackTrace();
        };
    }

    public LoopRunner loopRunner() {
        return LoopRunner.start(
                idleStrategy(),
                exceptionHandler(),
                runnable -> new Thread(null, runnable, "event-loop"),
                StepProvider.alwaysProvide(mainEventLoop())
        );
    }

    public MainEventLoop mainEventLoop() {
        if (mainEventLoop == null) {
            mainEventLoop = new MainEventLoop(
                    outputPollerStep(),
                    outputAppliedCondition(),
                    timerExpiryCheckerStep(),
                    inputPollerStep()
            );
        }
        return mainEventLoop;
    }

    private Step outputPollerStep() {
        final OutputQueue.Poller outputPoller = queueBuilder.outputPoller();
        final ApplicationHandler applicationHandler = applicationBuilder.applicationHandler();
        final LongConsumer lastAppliedOutputIndexConsumer = serverBuilder.lastAppliedOutputIndexConsumer();
        final Consumer<? super Event> leadershipChangeHandler = adminControllerBuilder.leadershipChangeEventConsumer();
        final Consumer<? super Event> timerStartStopHandler = timerControllerBuilder.startStopEventConsumer();
        final LongObjConsumer<Event> eventConsumer = (index, event) -> {
            try {
                leadershipChangeHandler.accept(event);
                timerStartStopHandler.accept(event);
                applicationHandler.applyOutputEvent(event);
                lastAppliedOutputIndexConsumer.accept(index);
            } catch (final Exception e) {
                //FIXME handle erors
                e.printStackTrace();
            }
        };
        return () -> outputPoller.poll(eventConsumer);
    }

    private BooleanSupplier outputAppliedCondition() {
        final ServerContext serverContext = serverBuilder.serverContext();
        return () -> serverContext.lastAppendedOutputQueueIndex() == serverContext.lastAppliedOutputQueueIndex();
    }

    private Step timerExpiryCheckerStep() {
        return timerControllerBuilder.expiryCheckerStep();
    }

    private Step inputPollerStep() {
        final InputQueue.Poller inputPoller = queueBuilder.inputPoller();
        final CommandController.Provider commandControllerProvider = commandHandlerBuilder.commandControllerProvider();
        final ApplicationHandler applicationHandler = applicationBuilder.applicationHandler();
        final Consumer<Event> eventConsumer = event -> {
            try (final CommandController commandController = commandControllerProvider.provideFor(event)) {
                applicationHandler.processInputEvent(event, commandController);
            } catch (final Exception e) {
                //FIXME handle erors
                e.printStackTrace();
            }
        };
        return () -> inputPoller.poll(eventConsumer);
    }
}
