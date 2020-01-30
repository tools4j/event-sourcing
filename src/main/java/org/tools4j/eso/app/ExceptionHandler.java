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

import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.evt.Event;
import org.tools4j.nobark.loop.Loop;
import org.tools4j.nobark.loop.Step;

@FunctionalInterface
public interface ExceptionHandler extends org.tools4j.nobark.loop.ExceptionHandler {

    void handleException(Command command, Throwable t);

    default void handleException(final Event event, final Throwable t) {
        System.err.println("Unhandled exception when applying event [" + event + "], e=" + t);
        t.printStackTrace();
    }

    @Override
    default void handleException(final Loop loop, final Step step, final Throwable t) {
        System.err.println("Unhandled exception when performing step [" + step + "], e=" + t);
        t.printStackTrace();
    }

    ExceptionHandler DEFAULT = (command, t) -> {
        System.err.println("Unhandled exception when processing command [" + command + "], e=" + t);
        t.printStackTrace();
    };

}
