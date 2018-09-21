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
import org.tools4j.mmap.region.api.RegionFactory;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.nobark.loop.Step;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

public class RegionRingFactoryConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionRingFactoryConfig.class);

    private static final Supplier<RegionRingFactory> ASYNC = () -> {
        final List<Runnable> mutableMappingCommands = new CopyOnWriteArrayList<>();
        return RegionRingFactory.forAsync(RegionFactory.ASYNC_VOLATILE_STATE_MACHINE,
                mutableMappingCommands::add,
                () -> {
                    final Runnable[] mappingCommands = mutableMappingCommands.toArray(new Runnable[mutableMappingCommands.size()]);

                    final Step step = () -> {
                        for (final Runnable mappingCommand : mappingCommands) {
                            mappingCommand.run();
                        }
                        return true;
                    };

                    new ThreadService(
                            "RegionMapper",
                            (name, threadRunCondition) ->
                                    new WhileLoop(name,
                                            () -> true,
                                            new BusySpinIdleStrategy()::idle,
                                            (message, exception) -> LOGGER.error("{} {}", message, exception),
                                            step
                                    ),
                            (name, runnable) -> {
                                final Thread thread = new Thread(null, runnable, name);
                                thread.setDaemon(true);
                                return thread;
                            }
                    );
                });
    };

    private static final Supplier<RegionRingFactory> SYNC = () -> RegionRingFactory.forSync(RegionFactory.SYNC);

    public static RegionRingFactory get(final String name) {
        switch (name) {
            case "SYNC": return SYNC.get();
            case "ASYNC" :return ASYNC.get();
            default: throw new IllegalArgumentException("Unknown regionRingFactory name " + name);
        }
    }
}
