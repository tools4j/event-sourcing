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

import org.agrona.DirectBuffer;
import org.tools4j.eventsourcing.api.IndexedMessageConsumer;
import org.tools4j.eventsourcing.sbe.AppendRequestDecoder;
import org.tools4j.eventsourcing.sbe.AppendResponseDecoder;
import org.tools4j.eventsourcing.sbe.VoteRequestDecoder;
import org.tools4j.eventsourcing.sbe.VoteResponseDecoder;

public interface ServerState extends IndexedMessageConsumer {
    Role role();
    default void onTransition() {}
    default void accept(final int source, final long sourceSeq, final long timeNanos, final DirectBuffer buffer, final int offset, final int length) {}
    default Transition processTick() {return Transition.STEADY;}
    default Transition onVoteRequest(final VoteRequestDecoder voteRequestDecoder) {return Transition.STEADY;}
    default Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {return Transition.STEADY;}
    default Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {return Transition.STEADY;}
    default Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {return Transition.STEADY;}
    default Transition onTimeoutNow() {return Transition.STEADY;}
}
