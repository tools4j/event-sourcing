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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.eventsourcing.api.MessageConsumer;
import org.tools4j.eventsourcing.api.Poller;
import org.tools4j.eventsourcing.common.PollingProcessStep;
import org.tools4j.eventsourcing.mmap.TestUtil;
import org.tools4j.eventsourcing.sbe.test.MessageHeaderDecoder;
import org.tools4j.eventsourcing.sbe.test.UpdateEventDecoder;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.mmap.region.impl.MappedFile;
import org.tools4j.nobark.loop.Step;
import org.tools4j.nobark.loop.StoppableThread;

public class DumpLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpLog.class);

    //@Test
    public void dumpLog() throws Exception {
        final String directory = System.getProperty("user.dir") + "/build";
        final String filePrefix = "raft_log";
        final int serverId = 2;
        final RegionRingFactory regionRingFactory = RegionRingFactory.sync();
        final int regionSize = (int) MappedFile.REGION_SIZE_GRANULARITY * 1024 * 4;
        final int regionRingSize = 4;
        final int regionsToMapAhead = 1;


        final MmapRaftPoller poller = new MmapRaftPoller(
                RaftRegionAccessorSupplier.forReadOnly(
                    directory,
                    filePrefix+ "_" + serverId,
                    regionRingFactory,
                    regionSize,
                    regionRingSize,
                    regionsToMapAhead),
                Poller.Options.builder().build());

        final MessageHeaderDecoder eventHeaderDecoder = new MessageHeaderDecoder();
        final UpdateEventDecoder updateDecoder = new UpdateEventDecoder();
        final StringBuilder sb = new StringBuilder();


        final MessageConsumer resultReader = (buf, ofst, len) -> {
            eventHeaderDecoder.wrap(buf, ofst);
            switch (eventHeaderDecoder.templateId()) {
                case UpdateEventDecoder.TEMPLATE_ID:
                    updateDecoder.wrap(buf, ofst + eventHeaderDecoder.encodedLength(),
                            eventHeaderDecoder.blockLength(), eventHeaderDecoder.schemaId());

                    sb.setLength(0);
                    updateDecoder.appendTo(sb);
                    LOGGER.info("Event: {}", sb);
            }
        };

        final Step step = new PollingProcessStep(poller, resultReader);

        final StoppableThread dumpLogService =  TestUtil.startService("dumpLog" + serverId, step, () -> false);

        dumpLogService.join(10000);
    }
}
