/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eventsourcing.core;

import org.tools4j.nobark.loop.Step;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * The event sourcing main event loop performs 3 parts:
 * <pre>
 *     (1) steps that should always be performed, such as
 *           - polling input events from the messaging transport and storing them in
 *             the input queue
 *           - appending replicated output events to the output queue in follower mode
 *           - processing of shutdown events
 *     (2) the output poller step which may result in application state updates if
 *         anything is processed
 *     (3) steps that should only be performed if (2) has no further events to
 *         process, e.g. polling and processing of an input event
 * </pre>
 */
public class MainEventLoop implements Step {

    private final Step outputPollerStep;
    private final BooleanSupplier outputAppliedCondition;
    private final Step[] otherSteps;

    public MainEventLoop(final Step outputPollerStep,
                         final BooleanSupplier outputAppliedCondition,
                         final Step... otherSteps) {
        this.outputPollerStep = Objects.requireNonNull(outputPollerStep);
        this.outputAppliedCondition = Objects.requireNonNull(outputAppliedCondition);
        this.otherSteps = Objects.requireNonNull(otherSteps);
    }

    @Override
    public boolean perform() {
        if (outputPollerStep.perform()) {
            return true;
        }
        if (outputAppliedCondition.getAsBoolean()) {
            boolean workDone = false;
            for (final Step step : otherSteps) {
                workDone |= step.perform();
            }
            return workDone;
        }
        return false;
    }

}
