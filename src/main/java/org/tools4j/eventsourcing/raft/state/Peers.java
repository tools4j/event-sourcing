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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;

public interface Peers {
    int ALL = -1;

    int majority();

    int peersMajority();

    Peer peer(int serverId);

    void resetAsFollowers(long nextIndex);

    void reset();

    void forEach(Consumer<? super Peer> consumer);

    <T> void forEach(T value, BiConsumer<T, ? super Peer> consumer);

    long majorityCommitIndex(long leaderCommitIndex,
                             int currentTerm,
                             LongToIntFunction termAtIndex);

    boolean majorityOfVotes();

    static void forEachPeer(final int clusterSize, final int leaderId, final IntConsumer peerConsumer) {
        IntStream.range(0, clusterSize)
                .filter(destinationId -> destinationId != leaderId)
                .forEach(peerConsumer);
    }

    /**
     * @return matchIndex that matchIndex = nextIndex - 1 at all peers, otherwise return -1 (Peer.NULL_INDEX)
     */
    long matchIndexPrecedingNextIndexAndEqualAtAllPeers();
}
