/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j, Marco Terzer, Anton Anufriev
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
package org.tools4j.eventsourcing.common;

import org.tools4j.nobark.loop.Service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

public class ThreadService implements Service {
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 2000;
    private final String name;
    private final long shutdownTimeoutMillis;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ThreadService(final String name,
                         final long shutdownTimeoutMillis,
                         final BiFunction<String, BooleanSupplier, Runnable> runnableFactory,
                         final BiFunction<String, Runnable, Thread> threadFactory) {

        this.name = Objects.requireNonNull(name);
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
        this.thread = threadFactory.apply(name, runnableFactory.apply(name, running::get));
        this.thread.start();
    }

    public ThreadService(final String name,
                         final BiFunction<String, BooleanSupplier, Runnable> runnableFactory,
                         final BiFunction<String, Runnable, Thread> threadFactory) {
        this(name, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, runnableFactory, threadFactory);
    }

    private void shutdown(final long shutdownTimeoutMillis) {
        running.set(false);
        try {
            thread.join(shutdownTimeoutMillis);
            thread.interrupt();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for service thread " + name + " to stop", e);
        }
    }

    @Override
    public void shutdown() {
        shutdown(shutdownTimeoutMillis);
    }

    @Override
    public void shutdownNow() {
        shutdown();
    }

    @Override
    public boolean isShutdown() {
        return !thread.isAlive() || !running.get();
    }

    @Override
    public boolean isTerminated() {
        return !thread.isAlive();
    }

    @Override
    public boolean awaitTermination(final long time, final TimeUnit timeUnit) {
        try {
            thread.join(timeUnit.toMillis(time));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for service thread " + name + " to stop", e);
        }
        return isTerminated();
    }
}
