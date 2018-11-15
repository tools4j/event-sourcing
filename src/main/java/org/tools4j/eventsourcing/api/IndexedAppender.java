package org.tools4j.eventsourcing.api;

public interface IndexedAppender extends IndexedMessageConsumer {
    long lastSourceSeq(int source);
}
