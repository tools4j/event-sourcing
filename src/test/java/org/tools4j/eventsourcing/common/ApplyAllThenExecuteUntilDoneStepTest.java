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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.tools4j.nobark.loop.Step;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ApplyAllThenExecuteUntilDoneStepTest {
    @Mock
    private Step inStep;
    @Mock
    private Step outStep;

    @Test
    public void perform() throws Exception {

        when(outStep.perform()).thenReturn(true, true, false, true, false);
        when(inStep.perform()).thenReturn(false, false, true);

        final ApplyAllThenExecuteUntilDoneStep processorStep = new ApplyAllThenExecuteUntilDoneStep(inStep::perform, outStep::perform);


        processorStep.perform();
        processorStep.perform();
        processorStep.perform();
        processorStep.perform();
        processorStep.perform();
        processorStep.perform();
        processorStep.perform();
        processorStep.perform();

        verify(outStep, times(5)).perform();
        verify(inStep, times(3)).perform();
    }

}