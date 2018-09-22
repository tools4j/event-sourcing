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
package org.tools4j.eventsourcing.mmap;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.common.ThreadService;
import org.tools4j.eventsourcing.common.WhileLoop;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Service;
import org.tools4j.nobark.loop.Step;

import java.util.function.BooleanSupplier;

public class TestUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtil.class);

    public static Service startService(final String serviceName, final Step step, final BooleanSupplier stopCondition) {
        return new ThreadService(
                serviceName,
                (name, threadRunCondition) ->
                        new WhileLoop(name,
                                () -> threadRunCondition.getAsBoolean() && !stopCondition.getAsBoolean(),
                                new BusySpinIdleStrategy()::idle,
                                (message, exception) -> LOGGER.error("{} {}", message, exception),
                                step
                        ),
                (name, runnable) -> {
                    final Thread thread = new Thread(null, runnable, name);
                    thread.setDaemon(false);
                    return thread;
                }
        );
    }

    public static RegionRingFactory getRegionRingFactory(final String[] args) {
        final String errorMessage = "Please specify a type of mapping (ASYNC/SYNC) as first program argument";
        if (args.length < 1) {
            throw new IllegalArgumentException(errorMessage);
        }
        try {
            return RegionRingFactoryConfig.get(args[0]);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
