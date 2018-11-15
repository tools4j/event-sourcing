package org.tools4j.eventsourcing.api;

/**
 * Factory for event applier. It should be implemented by the user of the library.
 * Event Applier applies the events to the application state.
 * It should be free of side-effects.
 */
public interface EventApplierFactory {
    EventApplierFactory NO_OP = (currentProgressState,
                                 completedProgressState) -> MessageConsumer.NO_OP;
    /**
     * Constructs a new event applier.
     * @param currentProgressState - current progress state
     * @param completedProgressState - completed progress state
     * @return event applier
     */
    MessageConsumer create(ProgressState currentProgressState,
                           ProgressState completedProgressState);
}
