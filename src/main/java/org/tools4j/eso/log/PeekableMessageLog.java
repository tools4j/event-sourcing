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
package org.tools4j.eso.log;

import static java.util.Objects.requireNonNull;

public interface PeekableMessageLog<M> extends MessageLog<M> {

    @Override
    PeekablePoller<M> poller();

    @FunctionalInterface
    interface PeekablePoller<M> extends Poller<M> {
        int peekOrPoll(final PeekPollHandler<? super M> handler);

        //not garbage free, better to override
        @Override
        default int poll(final Handler<? super M> handler) {
            requireNonNull(handler);
            return peekOrPoll(msg -> {
                handler.onMessage(msg);
                return PeekPollHandler.Result.POLL;
            });
        }
    }

    @FunctionalInterface
    interface PeekPollHandler<M> {
        enum Result {
            PEEK, POLL
        }
        Result onMessage(M message);
    }
}
