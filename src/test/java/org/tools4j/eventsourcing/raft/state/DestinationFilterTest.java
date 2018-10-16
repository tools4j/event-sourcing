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
package org.tools4j.eventsourcing.raft.state;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.tools4j.eventsourcing.sbe.HeaderDecoder;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DestinationFilterTest {
    private int serverId = 1;
    @Mock
    private HeaderDecoder headerDecoder;
    private Predicate<HeaderDecoder> destinationFilter;

    @Before
    public void setUp() throws Exception {
        destinationFilter = DestinationFilter.forServer(serverId);
    }

    @Test
    public void test_should_return_true_when_header_destinationId_matches_serverId() throws Exception {
        when(headerDecoder.destinationId()).thenReturn(serverId);
        assertThat(destinationFilter.test(headerDecoder)).isTrue();
    }

    @Test
    public void test_should_return_false_when_header_destinationId_matches_serverId() throws Exception {
        when(headerDecoder.destinationId()).thenReturn(2);
        assertThat(destinationFilter.test(headerDecoder)).isFalse();
    }
}