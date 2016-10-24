/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 mmap (tools4j), Marco Terzer
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
package org.tools4j.mmap.io;

import org.tools4j.mmap.queue.Appender;

/**
 * Message writer offers methods to write different value types for elements of a message.
 */
public interface MessageWriter<T> {
    MessageWriter<T> putBoolean(boolean value);
    MessageWriter<T> putInt8(byte value);
    MessageWriter<T> putInt8(int value);
    MessageWriter<T> putInt16(short value);
    MessageWriter<T> putInt16(int value);
    MessageWriter<T> putInt32(int value);
    MessageWriter<T> putInt64(long value);
    MessageWriter<T> putFloat32(float value);
    MessageWriter<T> putFloat64(double value);
    MessageWriter<T> putCharAscii(char value);
    MessageWriter<T> putChar(char value);
    MessageWriter<T> putStringAscii(CharSequence value);
    MessageWriter<T> putStringUtf8(CharSequence value);
    MessageWriter<T> putString(CharSequence value);
    T finishAppendMessage();
}
