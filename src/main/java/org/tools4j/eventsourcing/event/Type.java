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

public enum Type {
    INITIALIZE((byte)0),
    HEARTBEAT((byte)1),
    LEADERSHIP_FORCED((byte)2),
    LEADERSHIP_TRANSITION((byte)3),
    SHUTDOWN((byte)4),
    DATA((byte)5);

    private final byte code;
    Type(final byte code) {
        this.code = code;
    }
    public byte code() {
        return code;
    }
    public boolean isData() {
        return this == DATA;
    }
    public boolean isAdmin() {
        return this != DATA;
    }
    public boolean isLeadershipChange() {
        return this == INITIALIZE | this == LEADERSHIP_FORCED | this == LEADERSHIP_TRANSITION;
    }
    public static Type valueByCode(final byte code) {
        return VALUES[(int)code];
    }
    private static final Type[] VALUES = values();
}
