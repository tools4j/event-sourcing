package org.tools4j.eventsourcing.raft.api;

import org.tools4j.eventsourcing.api.IndexedMessageConsumer;

@FunctionalInterface
public interface OnTransitionHandler {
    void handle(int serverId, IndexedMessageConsumer consumer);

    default OnTransitionHandler andThen(final OnTransitionHandler next) {
        return (serverId, consumer) -> {
            handle(serverId, consumer);
            next.handle(serverId, consumer);
        };
    }
}
