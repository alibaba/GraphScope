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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Adds some functionality to ByteArrayOutputStream,
 * such as an option to write int value over previously written data
 * and directly get the byte array.
 */
public class ExtendedByteArrayDataOutput extends ByteArrayOutputStream
    implements ExtendedDataOutput {
    /** Default number of bytes */
    private static final int DEFAULT_BYTES = 32;
    /** Internal data output */
    private final DataOutput dataOutput;

    /**
     * Uses the byte array provided or if null, use a default size
     *
     * @param buf Buffer to use
     */
    public ExtendedByteArrayDataOutput(byte[] buf) {
        if (buf == null) {
            this.buf = new byte[DEFAULT_BYTES];
        } else {
            this.buf = buf;
        }
        dataOutput = new DataOutputStream(this);
    }

    /**
     * Uses the byte array provided at the given pos
     *
     * @param buf Buffer to use
     * @param pos Position in the buffer to start writing from
     */
    public ExtendedByteArrayDataOutput(byte[] buf, int pos) {
        this(buf);
        this.count = pos;
    }

    /**
     * Creates a new byte array output stream. The buffer capacity is
     * initially 32 bytes, though its size increases if necessary.
     */
    public ExtendedByteArrayDataOutput() {
        this(DEFAULT_BYTES);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of
     * the specified size, in bytes.
     *
     * @param size the initial size.
     * @exception  IllegalArgumentException if size is negative.
     */
    public ExtendedByteArrayDataOutput(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: " +
                size);
        }
        buf = new byte[size];
        dataOutput = new DataOutputStream(this);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        dataOutput.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        dataOutput.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        dataOutput.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        dataOutput.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        dataOutput.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        dataOutput.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        dataOutput.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        dataOutput.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        dataOutput.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        dataOutput.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        dataOutput.writeUTF(s);
    }

    @Override
    public void ensureWritable(int minSize) {
        if ((count + minSize) > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, count + minSize));
        }
    }

    @Override
    public void skipBytes(int bytesToSkip) {
        ensureWritable(bytesToSkip);
        count += bytesToSkip;
    }

    @Override
    public void writeInt(int position, int value) {
        if (position + 4 > count) {
            throw new IndexOutOfBoundsException(
                "writeIntOnPosition: Tried to write int to position " + position +
                    " but current length is " + count);
        }
        buf[position] = (byte) ((value >>> 24) & 0xFF);
        buf[position + 1] = (byte) ((value >>> 16) & 0xFF);
        buf[position + 2] = (byte) ((value >>> 8) & 0xFF);
        buf[position + 3] = (byte) ((value >>> 0) & 0xFF);
    }

    @Override
    public byte[] toByteArray(int offset, int length) {
        if (offset + length > count) {
            throw new IndexOutOfBoundsException(String.format("Offset: %d + " +
                "Length: %d exceeds the size of buf : %d", offset, length, count));
        }
        return Arrays.copyOfRange(buf, offset, length);
    }

    @Override
    public byte[] getByteArray() {
        return buf;
    }

    @Override
    public int getPos() {
        return count;
    }
}

