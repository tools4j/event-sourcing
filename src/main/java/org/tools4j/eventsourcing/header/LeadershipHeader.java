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
package org.tools4j.eventsourcing.header;

import org.tools4j.eventsourcing.event.Header;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;

public class LeadershipHeader extends AdminHeader {

    public static final short SUBTYPE_LEADERSHIP_TRANSITION = 0;
    public static final short SUBTYPE_LEADERSHIP_FORCED     = 1;

    private short subtypeId;

    public LeadershipHeader() {
        super(Type.LEADERSHIP);
    }

    @Override
    public short subtypeId() {
        return subtypeId;
    }

    public int leaderId() {
        return userData();
    }

    @Override
    public LeadershipHeader version(final Version version) {
        super.version(version);
        return this;
    }

    @Override
    public LeadershipHeader version(final byte version) {
        super.version(version);
        return this;
    }

    public LeadershipHeader subtypeId(final short subtypeId) {
        if (subtypeId != SUBTYPE_LEADERSHIP_TRANSITION & subtypeId != SUBTYPE_LEADERSHIP_FORCED) {
            throw new IllegalArgumentException("Invalid subtypeId: " + subtypeId);
        }
        this.subtypeId = subtypeId;
        return this;
    }

    @Override
    public LeadershipHeader inputSourceId(final int inputSourceId) {
        super.inputSourceId(inputSourceId);
        return this;
    }

    @Override
    public LeadershipHeader sourceSeqNo(final long sourceSeqNo) {
        super.sourceSeqNo(sourceSeqNo);
        return this;
    }

    @Override
    public LeadershipHeader eventTimeNanosSinceEpoch(final long eventTimeNanosSinceEpoch) {
        super.eventTimeNanosSinceEpoch(eventTimeNanosSinceEpoch);
        return this;
    }

    @Override
    public LeadershipHeader userData(final int userData) {
        super.userData(userData);
        return this;
    }

    public LeadershipHeader leaderId(final int leaderId) {
        return userData(leaderId);
    }

    @Override
    public LeadershipHeader init(final Header header) {
        super.init(header);
        return subtypeId(header.subtypeId());
    }
}
