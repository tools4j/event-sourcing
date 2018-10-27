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
package org.tools4j.eventsourcing.raft.mmap;


import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.TestMessage;
import org.tools4j.eventsourcing.api.BufferPoller;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.mmap.RegionRingFactoryConfig;
import org.tools4j.eventsourcing.raft.api.RaftLog;
import org.tools4j.eventsourcing.sbe.RaftIndexDecoder;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.mmap.region.impl.MappedFile;

import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

public class MmapRaftLogTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MmapRaftLogTest.class);

    private RaftLog raftLog;
    private Poller raftLogPoller;
    private LongConsumer truncateHandler = size -> {};

    @Before
    public void setUp() throws Exception {
        final String directory = System.getProperty("user.dir") + "/build";

        final int regionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 1024 * 4;
        LOGGER.info("regionSize: {}", regionSize);

        final RegionRingFactory regionRingFactory = RegionRingFactoryConfig.get("SYNC");

        final int ringSize = 4;
        final int regionsToMapAhead = 1;
        final long maxFileSize = 64L * 16 * 1024 * 1024 * 4;

        raftLog = new MmapRaftLog(
                RaftRegionAccessorSupplier.forReadWriteClear(
                        directory,
                        "raftLog",
                        regionRingFactory,
                        regionSize,
                        ringSize,
                        regionsToMapAhead,
                        maxFileSize),
                truncateHandler
        );

        raftLogPoller = new MmapRaftPoller(
                RaftRegionAccessorSupplier.forReadOnly(
                    directory,
                    "raftLog",
                    regionRingFactory,
                    regionSize,
                    ringSize,
                    regionsToMapAhead),
                Poller.Options.builder().bufferPoller(BufferPoller.PASS_THROUGH).build());
    }

    @After
    public void tearDown() throws Exception {
        raftLogPoller.close();
        raftLog.close();
    }

    @Test
    public void appending_wrapping_and_polling() throws Exception {
        //when empty log
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            Assert.fail("Should not have been invoked");
        })).isEqualTo(0);

        //then
        assertThat(raftLog.size()).isEqualTo(0);
        assertThat(raftLog.lastIndex()).isEqualTo(RaftLog.NULL_INDEX);
        assertThat(raftLog.lastTerm()).isEqualTo(RaftLog.NULL_TERM);

        //given
        final UnsafeBuffer payloadBuffer = new UnsafeBuffer();
        final RaftIndexDecoder raftIndexDecoder = new RaftIndexDecoder();

        //===========
        //and given
        final int term1 = 1;
        final int source1 = 10;
        final long sequence1 = 345;
        final long time1 = 1000;
        final TestMessage testMessage1 = TestMessage.forString("Test Message 1");

        //when
        raftLog.append(term1, source1, sequence1, time1, testMessage1.buffer, testMessage1.offset, testMessage1.length);

        // and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            assertThat(buffer.getStringWithoutLengthAscii(offset, length)).isEqualTo(
                    testMessage1.buffer.getStringWithoutLengthAscii(testMessage1.offset, testMessage1.length));
        })).isEqualTo(1);

        assertThat(raftLog.size()).isEqualTo(1);
        assertThat(raftLog.lastIndex()).isEqualTo(0);
        assertThat(raftLog.lastTerm()).isEqualTo(term1);

        //when
        raftLog.wrap(0, raftIndexDecoder, payloadBuffer);

        //then
        assertThat(raftIndexDecoder.length()).isEqualTo(testMessage1.length);
        assertThat(payloadBuffer.getStringWithoutLengthAscii(0, payloadBuffer.capacity())).isEqualTo(
                testMessage1.buffer.getStringWithoutLengthAscii(testMessage1.offset, testMessage1.length));

        assertThat(raftIndexDecoder.position()).isEqualTo(0);
        assertThat(raftIndexDecoder.source()).isEqualTo(source1);
        assertThat(raftIndexDecoder.sourceSeq()).isEqualTo(sequence1);
        assertThat(raftIndexDecoder.term()).isEqualTo(term1);

        //============================
        //when
        raftLog.truncate(0);

        //then
        assertThat(raftLog.size()).isEqualTo(0);
        assertThat(raftLog.lastIndex()).isEqualTo(RaftLog.NULL_INDEX);
        assertThat(raftLog.lastTerm()).isEqualTo(RaftLog.NULL_TERM);

        //when and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            Assert.fail("Should not have been invoked");
        })).isEqualTo(0);

        //============================
        //given
        final int term2 = 2;
        final int source2 = 11;
        final long sequence2 = 346;
        final long time2 = 1001;

        final TestMessage testMessage2 = TestMessage.forString("Test Message  2");

        //when
        raftLog.append(term2, source2, sequence2, time2, testMessage2.buffer, testMessage2.offset, testMessage2.length);

        assertThat(raftLog.size()).isEqualTo(1);
        assertThat(raftLog.lastIndex()).isEqualTo(0);
        assertThat(raftLog.lastTerm()).isEqualTo(term2);

        //and
        raftLog.wrap(0, raftIndexDecoder, payloadBuffer);

        //then
        assertThat(raftIndexDecoder.length()).isEqualTo(testMessage2.length);
        assertThat(payloadBuffer.getStringWithoutLengthAscii(0, payloadBuffer.capacity())).isEqualTo(
                testMessage2.buffer.getStringWithoutLengthAscii(testMessage2.offset, testMessage2.length));

        assertThat(raftIndexDecoder.position()).isEqualTo(0);
        assertThat(raftIndexDecoder.source()).isEqualTo(source2);
        assertThat(raftIndexDecoder.sourceSeq()).isEqualTo(sequence2);
        assertThat(raftIndexDecoder.term()).isEqualTo(term2);

        //when and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            Assert.fail("Should not have been invoked");
        })).isEqualTo(0);

        //============================
        //given
        final int term3 = 3;
        final int source3 = 12;
        final long sequence3 = 347;
        final long time3 = 1003;

        final TestMessage testMessage3 = TestMessage.forString("Test Message   3");

        //when
        raftLog.append(term3, source3, sequence3, time3, testMessage3.buffer, testMessage3.offset, testMessage3.length);

        assertThat(raftLog.size()).isEqualTo(2);
        assertThat(raftLog.lastIndex()).isEqualTo(1);
        assertThat(raftLog.lastTerm()).isEqualTo(term3);

        //and
        raftLog.wrap(1, raftIndexDecoder, payloadBuffer);


        //then
        assertThat(raftIndexDecoder.length()).isEqualTo(testMessage3.length);
        assertThat(payloadBuffer.getStringWithoutLengthAscii(0, payloadBuffer.capacity())).isEqualTo(
                testMessage3.buffer.getStringWithoutLengthAscii(testMessage3.offset, testMessage3.length));

        assertThat(raftIndexDecoder.position()).isEqualTo(testMessage2.length);
        assertThat(raftIndexDecoder.source()).isEqualTo(source3);
        assertThat(raftIndexDecoder.sourceSeq()).isEqualTo(sequence3);
        assertThat(raftIndexDecoder.term()).isEqualTo(term3);

        //when and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            assertThat(buffer.getStringWithoutLengthAscii(offset, length)).isEqualTo(
                    testMessage3.buffer.getStringWithoutLengthAscii(testMessage3.offset, testMessage3.length));
        })).isEqualTo(1);

        //==================
        //when
        raftLog.truncate(1);

        //then
        assertThat(raftLog.size()).isEqualTo(1);
        assertThat(raftLog.lastIndex()).isEqualTo(0);
        assertThat(raftLog.lastTerm()).isEqualTo(term2);

        //when and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            Assert.fail("Should not have been invoked");
        })).isEqualTo(0);

        //============================
        //given
        final int term4 = 4;
        final int source4 = 13;
        final long sequence4 = 348;
        final long time4 = 1004;

        final TestMessage testMessage4 = TestMessage.forString("Test Message    4");

        //when
        raftLog.append(term4, source4, sequence4, time4, testMessage4.buffer, testMessage4.offset, testMessage4.length);

        assertThat(raftLog.size()).isEqualTo(2);
        assertThat(raftLog.lastIndex()).isEqualTo(1);
        assertThat(raftLog.lastTerm()).isEqualTo(term4);

        //and
        raftLog.wrap(1, raftIndexDecoder, payloadBuffer);

        //then
        assertThat(raftIndexDecoder.length()).isEqualTo(testMessage4.length);
        assertThat(payloadBuffer.getStringWithoutLengthAscii(0, payloadBuffer.capacity())).isEqualTo(
                testMessage4.buffer.getStringWithoutLengthAscii(testMessage4.offset, testMessage4.length));

        assertThat(raftIndexDecoder.position()).isEqualTo(testMessage2.length);
        assertThat(raftIndexDecoder.source()).isEqualTo(source4);
        assertThat(raftIndexDecoder.sourceSeq()).isEqualTo(sequence4);
        assertThat(raftIndexDecoder.term()).isEqualTo(term4);

        //when and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            Assert.fail("Should not have been invoked");
        })).isEqualTo(0);

        //============================
        //given
        final int term5 = 4;
        final int source5 = 14;
        final long sequence5 = 349;
        final long time5 = 1005;

        final TestMessage testMessage5 = TestMessage.forString("Test Message     5");

        //when
        raftLog.append(term5, source5, sequence5, time5, testMessage5.buffer, testMessage5.offset, testMessage5.length);

        assertThat(raftLog.size()).isEqualTo(3);
        assertThat(raftLog.lastIndex()).isEqualTo(2);
        assertThat(raftLog.lastTerm()).isEqualTo(term5);

        //and
        raftLog.wrap(2, raftIndexDecoder, payloadBuffer);

        //then
        assertThat(raftIndexDecoder.length()).isEqualTo(testMessage5.length);
        assertThat(payloadBuffer.getStringWithoutLengthAscii(0, payloadBuffer.capacity())).isEqualTo(
                testMessage5.buffer.getStringWithoutLengthAscii(testMessage5.offset, testMessage5.length));

        assertThat(raftIndexDecoder.position()).isEqualTo(testMessage2.length + testMessage4.length);
        assertThat(raftIndexDecoder.source()).isEqualTo(source5);
        assertThat(raftIndexDecoder.sourceSeq()).isEqualTo(sequence5);
        assertThat(raftIndexDecoder.term()).isEqualTo(term5);

        //when and then
        assertThat(raftLogPoller.poll((buffer, offset, length) -> {
            assertThat(buffer.getStringWithoutLengthAscii(offset, length)).isEqualTo(
                    testMessage5.buffer.getStringWithoutLengthAscii(testMessage5.offset, testMessage5.length));
        })).isEqualTo(1);

        //===================
        assertThat(raftLog.contains(1, term4)).isEqualTo(RaftLog.Containment.IN);
        assertThat(raftLog.contains(2, term5)).isEqualTo(RaftLog.Containment.IN);
        assertThat(raftLog.contains(3, term5)).isEqualTo(RaftLog.Containment.OUT);

        assertThat(raftLog.lastKeyCompareTo(3, term5)).isEqualTo(-1);
        assertThat(raftLog.lastKeyCompareTo(2, term5)).isEqualTo(0);
        assertThat(raftLog.lastKeyCompareTo(1, term5)).isEqualTo(1);

    }

    @Test
    public void currentTerm() throws Exception {
        //when
        raftLog.currentTerm(567);
        //then
        assertThat(raftLog.currentTerm()).isEqualTo(567);
    }

    @Test
    public void votedFor() throws Exception {
        //when
        raftLog.votedFor(4);
        //then
        assertThat(raftLog.votedFor()).isEqualTo(4);
    }

    @Test
    public void clearVoteForAndIncCurrentTerm() throws Exception {
        //when
        raftLog.clearVoteForAndIncCurrentTerm();

        //then
        assertThat(raftLog.hasNotVotedYet()).isTrue();
        assertThat(raftLog.currentTerm()).isEqualTo(1);
    }

    @Test
    public void name() throws Exception {
        //when
        raftLog.clearVoteForAndSetCurrentTerm(45);

        //then
        assertThat(raftLog.hasNotVotedYet()).isTrue();
        assertThat(raftLog.currentTerm()).isEqualTo(45);
    }
}