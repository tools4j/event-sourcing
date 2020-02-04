/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 tools4j.org (Marco Terzer, Anton Anufriev)
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
package org.tools4j.eso.init;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;
import org.tools4j.eso.app.Application;
import org.tools4j.eso.app.ExceptionHandler;
import org.tools4j.eso.app.SimpleApplication;
import org.tools4j.eso.cmd.Command;
import org.tools4j.eso.cmd.CommandLoopback;
import org.tools4j.eso.cmd.FlyweightCommand;
import org.tools4j.eso.evt.Event;
import org.tools4j.eso.evt.EventRouter;
import org.tools4j.eso.evt.FlyweightEvent;
import org.tools4j.eso.log.InMemoryLog;
import org.tools4j.eso.src.Source;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Objects.requireNonNull;

public class LauncherTest {

    private static int TYPE_STRING = 1;

    private Application application = new SimpleApplication(
            "simple-test-app",
            this::onCommand, this::onEvent, ExceptionHandler.DEFAULT
    );
    private Queue<String> strings = new ConcurrentLinkedQueue<>();
    private Source.Poller stringSourcePoller = new StringSourcePoller(strings);

    @Test
    public void launch() throws Exception {
        strings.add("1");
        strings.add("12");
        try (Launcher launcher = Launcher.launch(
                Context.create()
                        .application(application)
                        .source(1, stringSourcePoller)
                        .commandLog(new InMemoryLog<>(new FlyweightCommand()))
                        .eventLog(new InMemoryLog<>(new FlyweightEvent()))
        )) {
            //
            Thread.sleep(200);
            strings.add("123");
            Thread.sleep(500);
            strings.add("hello world");
            while (!strings.isEmpty()) {
                launcher.join(20);
            }
        }
    }

    private void onCommand(final Command command, final EventRouter router) {
        System.out.println("command: " + command + ", payload=" + payloadFor(command.type(), command.payload()));
        router.routeEvent(command.type(), command.payload(), 0, command.payload().capacity());
    }

    private void onEvent(final Event event, final CommandLoopback commandLoopback) {
        System.out.println("event: " + event + ", payload=" + payloadFor(event.type(), event.payload()));
    }

    private String payloadFor(final int type, final DirectBuffer payload) {
        if (type == TYPE_STRING) {
            return payload.getStringAscii(0);
        }
        return "(unknown)";
    }

    private static class StringSourcePoller implements Source.Poller {
        final Queue<String> strings;
        long seq = 0;

        StringSourcePoller(final Queue<String> strings) {
            this.strings = requireNonNull(strings);
        }

        @Override
        public int poll(final Source.Handler handler) {
            final String msg = strings.poll();
            if (msg != null) {
                final MutableDirectBuffer buffer = new ExpandableArrayBuffer(msg.length() + 4);
                final int length = buffer.putStringAscii(0, msg);
                handler.onMessage(++seq, TYPE_STRING, buffer, 0, length);
                return 1;
            }
            return 0;
        }
    }
}