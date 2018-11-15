package org.tools4j.eventsourcing.api;

/**
 * Factory of a command executor. It should be implemented by the user of the library.
 * Command executor executes incoming commands and produces state-changing events that are to be applied to
 * EventApplier.
 */
public interface CommandExecutorFactory {
    CommandExecutorFactory PASS_THROUGH = (eventApplier,
                                           currentProgressState,
                                           completedProgressState) -> eventApplier;
    CommandExecutorFactory NO_OP = (eventApplier,
                                    currentProgressState,
                                    completedProgressState) -> MessageConsumer.NO_OP;
    /**
     * Constructs a new command executor.
     * @param eventApplier - resulting event consumer which applies the events to the application.
     * @param currentProgressState - current progress state
     * @param completedProgressState - completed progress state
     * @return command executor.
     */
    MessageConsumer create(MessageConsumer eventApplier,
                           ProgressState currentProgressState,
                           ProgressState completedProgressState);
}
