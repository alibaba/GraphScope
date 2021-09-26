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

import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * Byte array input stream that uses Unsafe methods to deserialize much faster
 */
public abstract class UnsafeReads extends Input implements ExtendedDataInput {

    /**
     * Constructor
     *
     * @param length buf length
     */
    public UnsafeReads(int length) {
        limit = length;
    }

    /**
     * Constructor with offset
     *
     * @param offset offset in memory
     * @param length buf length
     */
    public UnsafeReads(long offset, int length) {
        position = (int) offset;
        limit = length;
    }

    /**
     * How many bytes are still available?
     *
     * @return Number of bytes available
     */
    public abstract int available();

    /**
     * What position in the stream?
     *
     * @return Position
     */
    public abstract int getPos();

    /**
     * Check whether there are enough remaining bytes for an operation
     *
     * @param requiredBytes Bytes required to read
     */
    @Override
    protected int require(int requiredBytes) {
        if (available() < requiredBytes) {
            throw new IndexOutOfBoundsException(
                    "require: Only "
                            + available()
                            + " bytes remaining, trying to read "
                            + requiredBytes);
        }
        return available();
    }

    @Override
    public int skipBytes(int n) {
        require(n);
        position += n;
        return n;
    }

    @Override
    public String readLine() throws IOException {
        // Note that this code is mostly copied from DataInputStream
        char[] tmpBuf = new char[128];

        int room = tmpBuf.length;
        int offset = 0;
        int c;

        loop:
        while (true) {
            c = readByte();
            switch (c) {
                case -1:
                case '\n':
                    break loop;
                case '\r':
                    int c2 = readByte();
                    if ((c2 != '\n') && (c2 != -1)) {
                        position -= 1;
                    }
                    break loop;
                default:
                    if (--room < 0) {
                        char[] replacebuf = new char[offset + 128];
                        room = replacebuf.length - offset - 1;
                        System.arraycopy(tmpBuf, 0, replacebuf, 0, offset);
                        tmpBuf = replacebuf;
                    }
                    tmpBuf[offset++] = (char) c;
                    break;
            }
        }
        if ((c == -1) && (offset == 0)) {
            return null;
        }
        return String.copyValueOf(tmpBuf, 0, offset);
    }

    @Override
    public String readUTF() throws IOException {
        // Note that this code is mostly copied from DataInputStream
        int utflen = readUnsignedShort();

        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int c;
        int char2;
        int char3;
        int count = 0;
        int chararrCount = 0;

        readFully(bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararrCount++] = (char) c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx */
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    }
                    chararr[chararrCount++] =
                            (char)
                                    (((c & 0x0F) << 12)
                                            | ((char2 & 0x3F) << 6)
                                            | ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararrCount);
    }
}
