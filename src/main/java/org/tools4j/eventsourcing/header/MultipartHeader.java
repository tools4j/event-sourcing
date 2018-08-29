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
import org.tools4j.eventsourcing.event.Multipart;
import org.tools4j.eventsourcing.event.Type;
import org.tools4j.eventsourcing.event.Version;

public class MultipartHeader extends AdminHeader implements Multipart {

    public MultipartHeader() {
        super(Type.MULTIPART);
    }

    @Override
    public int partCount() {
        return userData();
    }

    @Override
    public MultipartHeader version(final Version version) {
        super.version(version);
        return this;
    }

    @Override
    public MultipartHeader version(final byte version) {
        super.version(version);
        return this;
    }

    @Override
    public MultipartHeader sourceSeqNo(final long sourceSeqNo) {
        super.sourceSeqNo(sourceSeqNo);
        return this;
    }

    @Override
    public MultipartHeader eventTimeNanosSinceEpoch(final long eventTimeNanosSinceEpoch) {
        super.eventTimeNanosSinceEpoch(eventTimeNanosSinceEpoch);
        return this;
    }

    @Override
    public MultipartHeader userData(final int userData) {
        super.userData(validatePartCount(userData));
        return this;
    }

    public MultipartHeader partCount(final int partCount) {
        return userData(partCount);
    }

    @Override
    public MultipartHeader init(final Header header) {
        super.init(header);
        return this;
    }

    static int validatePartCount(final int partCount) {
        if (partCount >= 0) {
            return partCount;
        }
        throw new IllegalArgumentException("Part count cannot be negative: " + partCount);
    }

}
