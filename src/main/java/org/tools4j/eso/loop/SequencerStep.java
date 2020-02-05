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

import org.tools4j.eso.input.Input;
import org.tools4j.nobark.loop.Step;

import java.util.function.Function;

public final class SequencerStep implements Step {

    private final Input.Poller[] inputPollers;
    private final Input.Handler[] handlers;

    private int inputIndex = 0;

    public SequencerStep(final Function<? super Input, ? extends Input.Handler> handlerFactory,
                         final Input... inputs) {
        this.inputPollers = initPollersFor(inputs);
        this.handlers = initHandlersFor(handlerFactory, inputs);
    }

    @Override
    public boolean perform() {
        final int count = inputPollers.length;
        for (int i = 0; i < count; i++) {
            if (inputPollers[inputIndex].poll(handlers[inputIndex]) > 0) {
                return true;
            }
            inputIndex++;
            if (inputIndex >= count) {
                inputIndex = 0;
            }
        }
        return false;
    }

    private Input.Poller[] initPollersFor(final Input... inputs) {
        final Input.Poller[] pollers = new Input.Poller[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            pollers[i] = inputs[i].poller();
        }
        return pollers;
    }

    private Input.Handler[] initHandlersFor(final Function<? super Input, ? extends Input.Handler> handlerFactory,
                                            final Input... inputs) {
        final Input.Handler[] handlers = new Input.Handler[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            handlers[i] = handlerFactory.apply(inputs[i]);
        }
        return handlers;
    }
}
