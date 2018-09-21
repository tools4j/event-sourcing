/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016-2018 mmap (tools4j), Marco Terzer, Anton Anufriev
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.nobark.loop.IdleStrategy;
import org.tools4j.nobark.loop.Step;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

public class WhileLoop implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WhileLoop.class);

    private final String name;
    private final BooleanSupplier runCondition;
    private final IdleStrategy idleStrategy;
    private final BiConsumer<? super String, ? super Exception> exceptionHandler;
    private final Step step;

    public WhileLoop(final String name,
                     final BooleanSupplier runCondition,
                     final IdleStrategy idleStrategy,
                     final BiConsumer<? super String, ? super Exception> exceptionHandler,
                     final Step step) {
        this.name = Objects.requireNonNull(name);
        this.runCondition = Objects.requireNonNull(runCondition);
        this.idleStrategy = Objects.requireNonNull(idleStrategy);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
        this.step = Objects.requireNonNull(step);
    }

    @Override
    public void run() {
        LOGGER.info("Started {} loop", name);
        while (runCondition.getAsBoolean()) {
            idleStrategy.idle(performStep());
        }
        LOGGER.info("Finished {} loop", name);
    }

    private boolean performStep() {
        try {
            return step.perform();
        } catch (final Exception ex) {
            exceptionHandler.accept(name, ex);
            return false;
        }
    }
}
