package org.tools4j.eventsourcing.raft.mmap;

import org.agrona.MutableDirectBuffer;
import org.tools4j.eventsourcing.api.IndexedMessageConsumer;
import org.tools4j.eventsourcing.raft.api.OnTransitionHandler;
import org.tools4j.eventsourcing.sbe.MessageHeaderEncoder;
import org.tools4j.eventsourcing.sbe.NoopEncoder;

import java.util.Objects;

public class AppendNoopOnLeaderTransition implements OnTransitionHandler {
    private final NoopEncoder noopEncoder = new NoopEncoder();
    private final MutableDirectBuffer encoderBuffer;
    private final MessageHeaderEncoder messageHeaderEncoder;

    public AppendNoopOnLeaderTransition(final MutableDirectBuffer encoderBuffer,
                                        final MessageHeaderEncoder messageHeaderEncoder) {
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
    }

    @Override
    public void handle(final int serverId, final IndexedMessageConsumer consumer) {
        final int noopLength = noopEncoder.wrapAndApplyHeader(encoderBuffer, 0, messageHeaderEncoder).encodedLength() +
                messageHeaderEncoder.encodedLength();
        consumer.accept(0, 0, 0, encoderBuffer, 0, noopLength);
    }
}
