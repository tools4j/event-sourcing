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
package org.tools4j.eventsourcing.raft.api;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.tools4j.spockito.Spockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(Spockito.class)
public class RaftLogContainmentTest {
    @Mock
    private RaftLog raftLog;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    @Spockito.Unroll({
            "| index  |  term | logSize  | logTermAtIndex  | containment  |",
            "|--------|-------|----------|-----------------|--------------|",
            "| 12     |  5    | 13       | 5               | IN           |",
            "| 12     |  5    | 10       | -1              | OUT          |",
            "| 12     |  5    | 16       | 6               | CONFLICT     |",
     })
    public void containmentFor(final long index,
                               final int term,
                               final long logSize,
                               final int logTermAtIndex,
                               final RaftLog.Containment containment) throws Exception {
        when(raftLog.size()).thenReturn(logSize);
        when(raftLog.term(index)).thenReturn(logTermAtIndex);


        assertThat(RaftLog.Containment.of(index, term, raftLog)).isEqualTo(containment);
    }

}