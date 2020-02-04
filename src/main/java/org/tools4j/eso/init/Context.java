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

import org.agrona.concurrent.IdleStrategy;
import org.tools4j.eso.app.Application;
import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.evt.Event;
import org.tools4j.eso.log.MessageLog;
import org.tools4j.eso.log.PeekableMessageLog;
import org.tools4j.eso.src.Source;
import org.tools4j.eso.time.TimeSource;

import java.util.concurrent.ThreadFactory;

interface Context {
    Application application();
    Context application(Application application);

    Source[] sources();
    Context source(Source source);
    Context source(int id, Source.Poller poller);

    PeekableMessageLog<Command> commandLog();
    Context commandLog(String file);
    Context commandLog(PeekableMessageLog<Command> commandLog);

    MessageLog<Event> eventLog();
    Context eventLog(String file);
    Context eventLog(MessageLog<Event> eventLog);

    TimeSource timeSource();
    Context timeSource(TimeSource timeSource);

    IdleStrategy idleStrategy();
    Context idleStrategy(IdleStrategy idleStrategy);

    ThreadFactory threadFactory();
    Context threadFactory(String threadName);
    Context threadFactory(ThreadFactory threadFactory);

    Context validateAndPopulateDefaults();

    static Context create() {
        return new DefaultContext();
    }
}
