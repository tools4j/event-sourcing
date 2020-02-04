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
package org.tools4j.eso.app;

import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class StateStrategyApplication implements Application {

    private final String name;
    private final Supplier<CommandProcessor> commandProcessorSupplier;
    private final Supplier<EventApplier> eventApplierSupplier;
    private final ExceptionHandler exceptionHandler;

    public StateStrategyApplication(final String name,
                                    final Supplier<CommandProcessor> commandProcessorSupplier,
                                    final Supplier<EventApplier> eventApplierSupplier,
                                    final ExceptionHandler exceptionHandler) {
        this.name = requireNonNull(name);
        this.commandProcessorSupplier = requireNonNull(commandProcessorSupplier);
        this.eventApplierSupplier = requireNonNull(eventApplierSupplier);
        this.exceptionHandler = requireNonNull(exceptionHandler);
    }

    public static <R,W> Application create(final String name,
                                           final R readOnlyState,
                                           final W readWriteState,
                                           final Function<? super R, ? extends CommandProcessor> commandHandlerSupplier,
                                           final Function<? super W, ? extends EventApplier> eventApplierSupplier,
                                           final ExceptionHandler exceptionHandler) {
        requireNonNull(name);
        requireNonNull(readOnlyState);
        requireNonNull(readWriteState);
        requireNonNull(commandHandlerSupplier);
        requireNonNull(eventApplierSupplier);
        requireNonNull(exceptionHandler);
        return new StateStrategyApplication(
                name,
                () -> commandHandlerSupplier.apply(readOnlyState),
                () -> eventApplierSupplier.apply(readWriteState),
                exceptionHandler
        );
    }

    @Override
    public CommandProcessor commandProcessor() {
        return commandProcessorSupplier.get();
    }

    @Override
    public EventApplier eventApplier() {
        return eventApplierSupplier.get();
    }

    @Override
    public ExceptionHandler exceptionHandler() {
        return exceptionHandler;
    }

    @Override
    public String toString() {
        return name;
    }
}
