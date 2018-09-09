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
package org.tools4j.eventsourcing.core;

import org.tools4j.eventsourcing.application.ServerConfig;
import org.tools4j.eventsourcing.command.AdminCommands;
import org.tools4j.eventsourcing.event.Version;
import org.tools4j.eventsourcing.header.AdminHeader;
import org.tools4j.eventsourcing.header.LeadershipHeader;
import org.tools4j.eventsourcing.header.ShutdownHeader;

import java.util.Objects;
import java.util.function.Consumer;

public class DefaultAdminCommands implements AdminCommands {

    private final ServerConfig config;
    private final Consumer<? super AdminHeader> eventAppender;
    private final LeadershipHeader leadershipHeader = new LeadershipHeader();
    private final ShutdownHeader shutdownHeader = new ShutdownHeader();

    public DefaultAdminCommands(final ServerConfig config,
                                final Consumer<? super AdminHeader> eventAppender) {
        this.config = Objects.requireNonNull(config);
        this.eventAppender = Objects.requireNonNull(eventAppender);
        this.leadershipHeader.version(Version.current());
        this.shutdownHeader.version(Version.current());
    }

    @Override
    public void transferLeadershipTo(final int leaderId, final boolean force) {
        if (config.serverIndexOf(leaderId) < 0) {
            throw new IllegalArgumentException("Invalid server ID: " + leaderId);
        }
        leadershipHeader
                .subtypeId(force ? LeadershipHeader.SUBTYPE_LEADERSHIP_FORCED : LeadershipHeader.SUBTYPE_LEADERSHIP_TRANSITION)
                .leaderId(leaderId);
        eventAppender.accept(leadershipHeader);
    }

    @Override
    public void shutdown() {
        eventAppender.accept(shutdownHeader);
    }
}
