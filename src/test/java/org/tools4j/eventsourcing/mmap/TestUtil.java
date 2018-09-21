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
