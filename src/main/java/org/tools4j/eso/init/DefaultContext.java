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
package org.tools4j.eso.init;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.tools4j.eso.app.Application;
import org.tools4j.eso.cmd.AdminCommandProcessor;
import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.cmd.CommandLoopback;
import org.tools4j.eso.cmd.DefaultCommandLoopback;
import org.tools4j.eso.evt.AdminEventApplier;
import org.tools4j.eso.evt.Event;
import org.tools4j.eso.evt.EventRouter;
import org.tools4j.eso.evt.FlyweightEventRouter;
import org.tools4j.eso.log.MessageLog;
import org.tools4j.eso.log.PeekableMessageLog;
import org.tools4j.eso.loop.*;
import org.tools4j.eso.src.AdjustableSequenceGenerator;
import org.tools4j.eso.src.SequenceGenerator;
import org.tools4j.eso.src.SimpleSequenceGenerator;
import org.tools4j.eso.src.Source;
import org.tools4j.eso.state.*;
import org.tools4j.eso.time.DefaultTimerControl;
import org.tools4j.eso.time.TimeSource;
import org.tools4j.eso.time.TimerControl;
import org.tools4j.nobark.run.ThreadLike;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

final class DefaultContext implements Context {

    private Application application;
    private final List<Source> sources = new ArrayList<>();
    private PeekableMessageLog<Command> commandLog;
    private MessageLog<Event> eventLog;
    private SequenceGenerator adminSequenceGenerator = new SimpleSequenceGenerator();
    private AdjustableSequenceGenerator timerIdGenerator = new SimpleSequenceGenerator();
    private IdleStrategy idleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
    private ThreadFactory threadFactory;
    private ServerConfig serverConfig;
    private ServerState serverState;
    private EventApplicationState.Mutable eventApplicationState;
    private TimerState.Mutable timerState;
    private TimeSource timeSource;

    @Override
    public Context validateAndPopulateDefaults() {
        if (application == null) {
            throw new IllegalStateException("Application must be set");
        }
        if (commandLog == null) {
            throw new IllegalStateException("Command log must be set");
        }
        if (eventLog == null) {
            throw new IllegalStateException("Event log must be set");
        }
        if (threadFactory == null) {
            threadFactory(DutyCycle.DEFAULT_THREAD_NAME);
        }
        if (serverConfig == null) {
            serverConfig = ServerConfig.singleton();
        }
        if (serverState == null) {
            serverState = new DefaultServerState();
        }
        if (eventApplicationState == null) {
            eventApplicationState = new DefaultEventApplicationState();
        }
        if (timerState == null) {
            timerState = new SimpleTimerState(1, timerIdGenerator);
        }
        if (timeSource == null) {
            timeSource = System::currentTimeMillis;
        }
        return this;
    }

    @Override
    public Application application() {
        return application;
    }

    public Context application(final Application application) {
        this.application = requireNonNull(application);
        return this;
    }

    @Override
    public Source[] sources() {
        return sources.toArray(new Source[sources.size()]);
    }

    @Override
    public Context source(final Source source) {
        sources.add(source);
        return this;
    }

    @Override
    public Context source(final int id, final Source.Poller poller) {
        return source(Source.create(id, poller));
    }

    @Override
    public PeekableMessageLog<Command> commandLog() {
        return commandLog;
    }

    @Override
    public Context commandLog(final String file) {
        throw new IllegalStateException("not supported yet");
    }

    @Override
    public Context commandLog(final PeekableMessageLog<Command> commandLog) {
        this.commandLog = requireNonNull(commandLog);
        return this;
    }

    @Override
    public MessageLog<Event> eventLog() {
        return eventLog;
    }

    @Override
    public Context eventLog(final String file) {
        throw new IllegalStateException("not supported yet");
    }

    @Override
    public Context eventLog(final MessageLog<Event> eventLog) {
        this.eventLog = requireNonNull(eventLog);
        return this;
    }

    @Override
    public SequenceGenerator adminSequenceGenerator() {
        return adminSequenceGenerator;
    }

    @Override
    public Context adminSequenceGenerator(final SequenceGenerator adminSequenceGenerator) {
        this.adminSequenceGenerator = requireNonNull(adminSequenceGenerator);
        return this;
    }

    @Override
    public AdjustableSequenceGenerator timerIdGenerator() {
        return timerIdGenerator;
    }

    @Override
    public Context timerIdGenerator(final AdjustableSequenceGenerator timerIdGenerator) {
        this.timerIdGenerator = requireNonNull(timerIdGenerator);
        return this;
    }

    @Override
    public IdleStrategy idleStrategy() {
        return idleStrategy;
    }

    @Override
    public Context idleStrategy(final IdleStrategy idleStrategy) {
        this.idleStrategy = requireNonNull(idleStrategy);
        return this;
    }

    @Override
    public ThreadFactory threadFactory() {
        return threadFactory;
    }

    @Override
    public Context threadFactory(final ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;//null allowed here
        return this;
    }

    @Override
    public Context threadFactory(final String threadName) {
        return threadFactory(threadName == null ? null : r -> new Thread(null, r, threadName));
    }

    @Override
    public ServerConfig serverConfig() {
        return serverConfig;
    }

    @Override
    public ServerState serverState() {
        return serverState;
    }

    @Override
    public EventApplicationState.Mutable eventApplicationState() {
        return eventApplicationState;
    }

    @Override
    public TimerState.Mutable timerState() {
        return timerState;
    }

    @Override
    public TimeSource timeSource() {
        return timeSource;
    }

    static DutyCycle dutyCycle(final Context context) {
        return new DutyCycle(
                context.serverState(),
                sourcePollerStep(context),
                timerTriggerStep(context),
                commandProcessorStep(context),
                commandSkipperStep(context),
                eventApplierStep(context)
        );
    }

    static SourcePollerStep sourcePollerStep(final Context context) {
        return new SourcePollerStep(
                context.commandLog().appender(),
                context.timeSource(),
                context.sources()
        );
    }

    static TimerTriggerStep timerTriggerStep(final Context context) {
        return new TimerTriggerStep(
                context.commandLog().appender(),
                context.timeSource(),
                context.timerState(),
                context.adminSequenceGenerator()
        );
    }

    static CommandProcessorStep commandProcessorStep(final Context context) {
        final FlyweightEventRouter eventRouter = flyweightEventRouter(context);
        return new CommandProcessorStep(
                context.commandLog().poller(),
                eventRouter,
                timerControl(context, eventRouter),
                adminCommandProcessor(context),
                context.application()
        );
    }

    static CommandSkipperStep commandSkipperStep(final Context context) {
        return new CommandSkipperStep(
                context.commandLog().poller(),
                context.eventApplicationState()
         );
    }

    static EventApplierStep eventApplierStep(final Context context) {
        return new EventApplierStep(
                context.eventLog().poller(),
                commandLoopback(context),
                adminEventApplier(context),
                context.application()
        );
    }

    static FlyweightEventRouter flyweightEventRouter(final Context context) {
        return new FlyweightEventRouter(context.eventLog().appender());
    }

    static TimerControl timerControl(final Context context, final EventRouter eventRouter) {
        return new DefaultTimerControl(context.timerIdGenerator(), context.timerState(), eventRouter);
    }

    static AdminCommandProcessor adminCommandProcessor(final Context context) {
        return new AdminCommandProcessor();
    }

    static AdminEventApplier adminEventApplier(final Context context) {
        return new AdminEventApplier(context.timerState(), context.eventApplicationState());
    }

    static CommandLoopback commandLoopback(final Context context) {
        return new DefaultCommandLoopback(
                context.commandLog().appender(),
                context.timeSource(),
                context.adminSequenceGenerator()
        );
    }

    static org.tools4j.nobark.loop.IdleStrategy idleStrategy(final Context context) {
        final IdleStrategy idleStrategy = requireNonNull(context.idleStrategy());
        return new org.tools4j.nobark.loop.IdleStrategy() {
            @Override
            public void idle() {
                idleStrategy.idle();
            }

            @Override
            public void reset() {
                idleStrategy.reset();
            }

            @Override
            public void idle(final boolean workDone) {
                idleStrategy.idle(workDone ? 1 : 0);
            }

            @Override
            public String toString() {
                return idleStrategy.toString();
            }
        };
    }

    static ThreadLike start(final Context context) {
        context.validateAndPopulateDefaults();
        return dutyCycle(context).start(
                idleStrategy(context),
                context.application().exceptionHandler(),
                context.threadFactory()
        );
    }

}
