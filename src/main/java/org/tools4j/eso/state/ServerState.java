package org.tools4j.eso.state;

public interface ServerState {
    int serverId();
    int[] serverIds();
    int leaderId();
    int term();
    boolean processCommands();
}
