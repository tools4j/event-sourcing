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

import org.tools4j.nobark.loop.Step;

import java.util.Objects;

public final class DownstreamWhileDoneThenUpstreamUntilDoneStep implements Step {

    private final Step upstreamProcessStepState;
    private Step downstreamProcessStepState;
    private Step currentStep;


    public DownstreamWhileDoneThenUpstreamUntilDoneStep(final Step upstreamProcessStep, final Step downstreamProcessStep) {
        Objects.requireNonNull(upstreamProcessStep);
        Objects.requireNonNull(downstreamProcessStep);

        upstreamProcessStepState = () -> {
            final boolean workDone = upstreamProcessStep.perform();
            if (workDone) currentStep = downstreamProcessStepState;
            return workDone;
        };

        downstreamProcessStepState = () -> {
            final boolean workDone = downstreamProcessStep.perform();
            if (!workDone) currentStep = upstreamProcessStepState;
            return workDone;
        };

        currentStep = downstreamProcessStepState;
    }

    @Override
    public boolean perform() {
        return currentStep.perform();
    }
}
