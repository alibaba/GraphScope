/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.utils;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BOOLEAN;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_CHAR;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_DOUBLE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_FLOAT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_SHORT;

import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Byte array output stream that uses Unsafe methods to serialize/deserialize much faster.
 * <p>
 * This stream now extends com.esotericsoftware.kryo.io.Output so that kryo serialization can
 * directly write to this stream without using an additional buffer, providing a faster
 * serialization.
 * <p>
 * Users of this class has to explicitly close the stream to avoid style check errors even though
 * close is no-op when the underlying stream is not set.
 */
public class UnsafeByteArrayOutputStream extends Output implements ExtendedDataOutput {

    /**
     * Default number of bytes
     */
    private static final int DEFAULT_BYTES = 32;
    /**
     * Access to the unsafe class
     */
    private static final sun.misc.Unsafe UNSAFE;
    /**
     * Offset of a byte array
     */
    private static final long BYTE_ARRAY_OFFSET;

    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            // Checkstyle exception due to needing to check if unsafe is allowed
            // CHECKSTYLE: stop IllegalCatch
        } catch (Exception e) {
            // CHECKSTYLE: resume IllegalCatch
            throw new RuntimeException("UnsafeByteArrayOutputStream: Failed to " + "get unsafe", e);
        }
    }

    /**
     * Constructor
     */
    public UnsafeByteArrayOutputStream() {
        this(DEFAULT_BYTES);
    }

    /**
     * Constructor
     *
     * @param size Initial size of the underlying byte array
     */
    public UnsafeByteArrayOutputStream(int size) {
        buffer = new byte[size];
        capacity = size;
    }

    /**
     * Constructor to take in a buffer
     *
     * @param buf Buffer to start with, or if null, create own buffer
     */
    public UnsafeByteArrayOutputStream(byte[] buf) {
        if (buf == null) {
            this.buffer = new byte[DEFAULT_BYTES];
        } else {
            this.buffer = buf;
        }
        capacity = this.buffer.length;
    }

    /**
     * Constructor to take in a buffer with a given position into that buffer
     *
     * @param buf Buffer to start with
     * @param pos Position to write at the buffer
     */
    public UnsafeByteArrayOutputStream(byte[] buf, int pos) {
        this(buf);
        this.position = pos;
    }

    /**
     * Ensure that this buffer has enough remaining space to add the size. Creates and copies to a
     * new buffer if necessary
     *
     * @param size Size to add
     */
    @Override
    protected boolean require(int size) {
        if (position + size > buffer.length) {
            byte[] newBuf = new byte[(buffer.length + size) << 1];
            System.arraycopy(buffer, 0, newBuf, 0, position);
            buffer = newBuf;
            capacity = buffer.length;
            return true;
        }
        return false;
    }

    @Override
    public byte[] getByteArray() {
        return buffer;
    }

    @Override
    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, position);
    }

    @Override
    public byte[] toByteArray(int offset, int length) {
        if (offset + length > position) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Offset: %d + " + "Length: %d exceeds the size of buffer : %d",
                            offset, length, position));
        }
        return Arrays.copyOfRange(buffer, offset, length);
    }

    @Override
    public void reset() {
        position = 0;
    }

    @Override
    public int getPos() {
        return position;
    }

    @Override
    public void write(int b) {
        require(SIZE_OF_BYTE);
        buffer[position] = (byte) b;
        position += SIZE_OF_BYTE;
    }

    @Override
    public void write(byte[] b) {
        require(b.length);
        System.arraycopy(b, 0, buffer, position, b.length);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        require(len);
        System.arraycopy(b, off, buffer, position, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v) {
        require(SIZE_OF_BOOLEAN);
        UNSAFE.putBoolean(buffer, BYTE_ARRAY_OFFSET + position, v);
        position += SIZE_OF_BOOLEAN;
    }

    @Override
    public void writeByte(int v) {
        require(SIZE_OF_BYTE);
        UNSAFE.putByte(buffer, BYTE_ARRAY_OFFSET + position, (byte) v);
        position += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int v) {
        require(SIZE_OF_SHORT);
        UNSAFE.putShort(buffer, BYTE_ARRAY_OFFSET + position, (short) v);
        position += SIZE_OF_SHORT;
    }

    @Override
    public void writeChar(int v) throws IOException {
        require(SIZE_OF_CHAR);
        UNSAFE.putChar(buffer, BYTE_ARRAY_OFFSET + position, (char) v);
        position += SIZE_OF_CHAR;
    }

    @Override
    public void writeChar(char v) {
        require(SIZE_OF_CHAR);
        UNSAFE.putChar(buffer, BYTE_ARRAY_OFFSET + position, v);
        position += SIZE_OF_CHAR;
    }

    @Override
    public void ensureWritable(int minSize) {
        if ((position + minSize) > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, position + minSize));
        }
    }

    @Override
    public void skipBytes(int bytesToSkip) {
        ensureWritable(bytesToSkip);
        position += bytesToSkip;
    }

    @Override
    public void writeInt(int v) {
        require(SIZE_OF_INT);
        UNSAFE.putInt(buffer, BYTE_ARRAY_OFFSET + position, v);
        position += SIZE_OF_INT;
    }

    @Override
    public void writeInt(int pos, int value) {
        if (pos + SIZE_OF_INT > this.position) {
            throw new IndexOutOfBoundsException(
                    "writeInt: Tried to write int to position "
                            + pos
                            + " but current length is "
                            + this.position);
        }
        UNSAFE.putInt(buffer, BYTE_ARRAY_OFFSET + pos, value);
    }

    @Override
    public void writeLong(long v) {
        require(SIZE_OF_LONG);
        UNSAFE.putLong(buffer, BYTE_ARRAY_OFFSET + position, v);
        position += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float v) {
        require(SIZE_OF_FLOAT);
        UNSAFE.putFloat(buffer, BYTE_ARRAY_OFFSET + position, v);
        position += SIZE_OF_FLOAT;
    }

    @Override
    public void writeDouble(double v) {
        require(SIZE_OF_DOUBLE);
        UNSAFE.putDouble(buffer, BYTE_ARRAY_OFFSET + position, v);
        position += SIZE_OF_DOUBLE;
    }

    @Override
    public void writeBytes(String s) throws IOException {
        // Note that this code is mostly copied from DataOutputStream
        int len = s.length();
        require(len);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            writeByte(v);
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        // Note that this code is mostly copied from DataOutputStream
        int len = s.length();
        require(len * SIZE_OF_CHAR);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            writeChar(v);
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        // Note that this code is mostly copied from DataOutputStream
        int strlen = s.length();
        int utflen = 0;
        int c;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");
        }

        require(utflen + SIZE_OF_SHORT);
        writeShort(utflen);

        int i = 0;
        for (i = 0; i < strlen; i++) {
            c = s.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            buffer[position++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffer[position++] = (byte) c;

            } else if (c > 0x07FF) {
                buffer[position++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                buffer[position++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                buffer[position++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            } else {
                buffer[position++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                buffer[position++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            }
        }
    }
}
