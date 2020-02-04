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
package org.tools4j.eso.loop;

import org.tools4j.eso.src.Source;
import org.tools4j.nobark.loop.Step;

import java.util.function.Function;

public final class SequencerStep implements Step {

    private final Source.Poller[] sourcePollers;
    private final Source.Handler[] handlers;

    private int sourceIndex = 0;

    public SequencerStep(final Function<? super Source, ? extends Source.Handler> handlerFactory,
                         final Source... sources) {
        this.sourcePollers = initPollersFor(sources);
        this.handlers = initHandlersFor(handlerFactory, sources);
    }

    @Override
    public boolean perform() {
        final int count = sourcePollers.length;
        for (int i = 0; i < count; i++) {
            if (sourcePollers[sourceIndex].poll(handlers[sourceIndex]) > 0) {
                return true;
            }
            sourceIndex++;
            if (sourceIndex >= count) {
                sourceIndex = 0;
            }
        }
        return false;
    }

    private Source.Poller[] initPollersFor(final Source... sources) {
        final Source.Poller[] pollers = new Source.Poller[sources.length];
        for (int i = 0; i < sources.length; i++) {
            pollers[i] = sources[i].poller();
        }
        return pollers;
    }

    private Source.Handler[] initHandlersFor(final Function<? super Source, ? extends Source.Handler> handlerFactory,
                                             final Source... sources) {
        final Source.Handler[] handlers = new Source.Handler[sources.length];
        for (int i = 0; i < sources.length; i++) {
            handlers[i] = handlerFactory.apply(sources[i]);
        }
        return handlers;
    }
}
