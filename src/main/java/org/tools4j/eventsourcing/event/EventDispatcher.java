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
package org.tools4j.eventsourcing.event;

import org.tools4j.eventsourcing.header.*;

public class EventDispatcher {
    private final InitializeHeader.Default initializeHeader = InitializeHeader.create();
    private final HeartbeatHeader.Default heartbeatHeader = HeartbeatHeader.create();
    private final LeadershipHeader.Default leadershipHeader = LeadershipHeader.create();
    private final TimerHeader.Default timerHeader = TimerHeader.create();
    private final ShutdownHeader.Default shutdownHeader = ShutdownHeader.create();
    private final NoopHeader.Default noopHeader = NoopHeader.create();
    private final DataHeader.Default dataHeader = DataHeader.create();
    private final MultipartHeader.Default multipartHeader = MultipartHeader.create();
    private final DefaultPartEvent partEvent = new DefaultPartEvent();

    public void dispatch(final Event event) {
        switch (event.type()) {
            case INITIALIZE:
                onInitialize(initializeHeader.wrap(event.header()), event);
                break;
            case HEARTBEAT:
                onHeartbeat(heartbeatHeader.wrap(event.header()), event);
                break;
            case LEADERSHIP:
                onLeadership(leadershipHeader.wrap(event.header()), event);
                break;
            case TIMER:
                onTimer(timerHeader.wrap(event.header()), event);
                break;
            case SHUTDOWN:
                onShutdown(shutdownHeader.wrap(event.header()), event);
                break;
            case NOOP:
                onNoop(noopHeader.wrap(event.header()), event);
                break;
            case DATA:
                onData(dataHeader.wrap(event.header()), event);
                break;
            case MULTIPART:
                onMultipart(multipartHeader.wrap(event.header()), event);
                break;
            default:
                throw new IllegalArgumentException("Unsupported event type: " + event.type());
        }
    }

    public void onInitialize(final InitializeHeader header, final Event event) {
        //no op
    }
    public void onHeartbeat(final HeartbeatHeader header, final Event event) {
        //no op
    }
    public void onLeadership(final LeadershipHeader header, final Event event) {
        //no op
    }
    public void onTimer(final TimerHeader header, final Event event) {
        //no op
    }
    public void onShutdown(final ShutdownHeader header, final Event event) {
        //no op
    }
    public void onNoop(final NoopHeader header, final Event event) {
        //no op
    }
    public void onData(final DataHeader header, final Event event) {
        //no op
    }
    public void onMultipart(final MultipartHeader header, final Event event) {
        final int partCount = header.partCount();
        int offset = 0;
        for (int i = 0; i < partCount; i++) {
            partEvent.wrap(header, event.payload(), offset);
            offset += partEvent.totalLength();
            onPart(header, partEvent.header(), partEvent);
        }
    }
    public void onPart(final MultipartHeader header, final Multipart.Part partHeader, final Event partEvent) {
        dispatch(partEvent);
    }
}
