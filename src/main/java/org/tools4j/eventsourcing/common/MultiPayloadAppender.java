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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.tools4j.eventsourcing.api.IndexedAppender;
import org.tools4j.eventsourcing.api.Transaction;
import org.tools4j.eventsourcing.sbe.MessageHeaderEncoder;
import org.tools4j.eventsourcing.sbe.MultiPayloadEncoder;

import java.util.Objects;

/**
 * Appender that encodes multiple messages with SBE MultiPayloadEncoder and delegates appending of the encoded message to
 * delegateAppender.
 */
public final class MultiPayloadAppender implements Transaction {
    private static final int MAX_ENTRIES = 10;

    private final IndexedAppender delegateAppender;
    private final MutableDirectBuffer messageEncodingBuffer;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MultiPayloadEncoder multiPayloadEncoder = new MultiPayloadEncoder();

    private int source;
    private long sourceSeq;
    private long eventTimeNanos;
    private boolean allowEmpty;
    private int messageLength;
    private int entries;
    private int limitBeforeEntries;
    private MultiPayloadEncoder.EntriesEncoder entriesEncoder;

    public MultiPayloadAppender(final IndexedAppender delegateAppender,
                                final MutableDirectBuffer messageEncodingBuffer) {
        this.delegateAppender = Objects.requireNonNull(delegateAppender);
        this.messageEncodingBuffer = Objects.requireNonNull(messageEncodingBuffer);
    }

    @Override
    public void init(final int source, final long sourceSeq, final long eventTimeNanos, final boolean allowEmpty) {
        this.entries = 0;
        this.source  = source;
        this.sourceSeq = sourceSeq;
        this.eventTimeNanos = eventTimeNanos;
        this.allowEmpty = allowEmpty;

        final int headerLength = messageHeaderEncoder.wrap(messageEncodingBuffer, 0)
                .blockLength(MultiPayloadEncoder.BLOCK_LENGTH)
                .schemaId(MultiPayloadEncoder.SCHEMA_ID)
                .version(MultiPayloadEncoder.SCHEMA_VERSION)
                .templateId(MultiPayloadEncoder.TEMPLATE_ID)
                .encodedLength();

        messageLength = headerLength;
        multiPayloadEncoder.wrap(messageEncodingBuffer, headerLength);
        limitBeforeEntries = multiPayloadEncoder.limit();

        entriesEncoder = multiPayloadEncoder.entriesCount(MAX_ENTRIES);
    }

    @Override
    public boolean commit() {
        if (allowEmpty || entries > 0) {
            final int saveLimit = multiPayloadEncoder.limit();
            multiPayloadEncoder.limit(limitBeforeEntries);
            multiPayloadEncoder.entriesCount(entries);
            multiPayloadEncoder.limit(saveLimit);

            messageLength += multiPayloadEncoder.encodedLength();
            //messageLength = BitUtil.align(messageLength, 64);
            delegateAppender.accept(source, sourceSeq, eventTimeNanos, messageEncodingBuffer, 0, messageLength);
            return true;
        }
        return false;
    }

    @Override
    public long lastSourceSeq(final int source) {
        return delegateAppender.lastSourceSeq(source);
    }

    @Override
    public void accept(final DirectBuffer buffer, final int offset, final int length) {
        entriesEncoder = entriesEncoder.next().putValue(buffer, offset, length);
        entries++;
    }
}
