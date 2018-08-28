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
package org.tools4j.eventsourcing.application;

public interface ServerConfig {
    int instanceIndex();
    int instanceId();
    int serverCount();
    int serverId(int index);

    int inputSourceCount();
    int inputSourceId(int index);

    default int serverIndexOf(final int serverId) {
        final int count = serverCount();
        for (int i = 0; i < count; i++) {
            if (serverId(i) == serverId) {
                return i;
            }
        }
        return -1;
    }
    default int inputSourceIndexOf(final int inputSourceId) {
        final int count = inputSourceCount();
        for (int i = 0; i < count; i++) {
            if (inputSourceId(i) == inputSourceId) {
                return i;
            }
        }
        return -1;
    }
}
