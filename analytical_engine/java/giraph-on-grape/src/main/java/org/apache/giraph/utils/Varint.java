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
package org.apache.giraph.utils;

/**
 * This Code is adapted from main/java/org/apache/mahout/math/Varint.java
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>
 * Encodes signed and unsigned values using a common variable-length scheme,
 * found for example in <a
 * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
 * Google's Protocol Buffers</a>. It uses fewer bytes to encode smaller values,
 * but will use slightly more bytes to encode large values.
 * </p>
 * <p>
 * Signed values are further encoded using so-called zig-zag encoding in order
 * to make them "compatible" with variable-length encoding.
 * </p>
 */
public final class Varint {

    /**
     * private constructor
     */
    private Varint() {
    }

    /**
     * Encodes a value using the variable-length encoding from <a
     * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>.
     *
     * @param value to encode
     * @param out to write bytes to
     * @throws IOException
     *           if {@link DataOutput} throws {@link IOException}
     */
    private static void writeVarLong(
        long value,
        DataOutput out
    ) throws IOException {
        while (true) {
            int bits = ((int) value) & 0x7f;
            value >>>= 7;
            if (value == 0) {
                out.writeByte((byte) bits);
                return;
            }
            out.writeByte((byte) (bits | 0x80));
        }
    }

    /**
     * Encodes a value using the variable-length encoding from <a
     * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>.
     *
     * @param value to encode
     * @param out to write bytes to
     * @throws IOException
     *           if {@link DataOutput} throws {@link IOException}
     */
    public static void writeUnsignedVarLong(
        long value,
        DataOutput out
    ) throws IOException {
        if (value < 0) {
            throw new IllegalStateException(
                "Negative value passed into writeUnsignedVarLong - " + value);
        }
        writeVarLong(value, out);
    }

    /**
     * Zig-zag encoding for signed longs
     *
     * @param value to encode
     * @param out to write bytes to
     * @throws IOException
     *           if {@link DataOutput} throws {@link IOException}
     */
    public static void writeSignedVarLong(
        long value,
        DataOutput out
    ) throws IOException {
        writeVarLong((value << 1) ^ (value >> 63), out);
    }

    /**
     * @see #writeVarLong(long, DataOutput)
     * @param value to encode
     * @param out to write bytes to
     * @throws IOException
     */
    private static void writeVarInt(
        int value,
        DataOutput out
    ) throws IOException {
        while (true) {
            int bits = value & 0x7f;
            value >>>= 7;
            if (value == 0) {
                out.writeByte((byte) bits);
                return;
            }
            out.writeByte((byte) (bits | 0x80));
        }
    }

    /**
     * @see #writeVarLong(long, DataOutput)
     * @param value to encode
     * @param out to write bytes to
     * @throws IOException
     */
    public static void writeUnsignedVarInt(
        int value,
        DataOutput out
    ) throws IOException {
        if (value < 0) {
            throw new IllegalStateException(
                "Negative value passed into writeUnsignedVarInt - " + value);
        }
        writeVarInt(value, out);
    }

    /**
     * Zig-zag encoding for signed ints
     *
     * @see #writeUnsignedVarInt(int, DataOutput)
     * @param value to encode
     * @param out to write bytes to
     * @throws IOException
     */
    public static void writeSignedVarInt(
        int value,
        DataOutput out
    ) throws IOException {
        writeVarInt((value << 1) ^ (value >> 31), out);
    }

    /**
     * @param in to read bytes from
     * @return decode value
     * @throws IOException
     *           if {@link DataInput} throws {@link IOException}
     */
    public static long readUnsignedVarLong(DataInput in) throws IOException {
        long tmp;
        // CHECKSTYLE: stop InnerAssignment
        if ((tmp = in.readByte()) >= 0) {
            return tmp;
        }
        long result = tmp & 0x7f;
        if ((tmp = in.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    if ((tmp = in.readByte()) >= 0) {
                        result |= tmp << 28;
                    } else {
                        result |= (tmp & 0x7f) << 28;
                        if ((tmp = in.readByte()) >= 0) {
                            result |= tmp << 35;
                        } else {
                            result |= (tmp & 0x7f) << 35;
                            if ((tmp = in.readByte()) >= 0) {
                                result |= tmp << 42;
                            } else {
                                result |= (tmp & 0x7f) << 42;
                                if ((tmp = in.readByte()) >= 0) {
                                    result |= tmp << 49;
                                } else {
                                    result |= (tmp & 0x7f) << 49;
                                    if ((tmp = in.readByte()) >= 0) {
                                        result |= tmp << 56;
                                    } else {
                                        result |= (tmp & 0x7f) << 56;
                                        result |= ((long) in.readByte()) << 63;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // CHECKSTYLE: resume InnerAssignment
        return result;
    }

    /**
     * @param in to read bytes from
     * @return decode value
     * @throws IOException
     *           if {@link DataInput} throws {@link IOException}
     */
    public static long readSignedVarLong(DataInput in) throws IOException {
        long raw = readUnsignedVarLong(in);
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        return temp ^ (raw & (1L << 63));
    }

    /**
     * @throws IOException
     *           if {@link DataInput} throws {@link IOException}
     * @param in to read bytes from
     * @return decode value
     */
    public static int readUnsignedVarInt(DataInput in) throws IOException {
        int tmp;
        // CHECKSTYLE: stop InnerAssignment
        if ((tmp = in.readByte()) >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = in.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (in.readByte()) << 28;
                }
            }
        }
        // CHECKSTYLE: resume InnerAssignment
        return result;
    }

    /**
     * @throws IOException
     *           if {@link DataInput} throws {@link IOException}
     * @param in to read bytes from
     * @return decode value
     */
    public static int readSignedVarInt(DataInput in) throws IOException {
        int raw = readUnsignedVarInt(in);
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        return temp ^ (raw & (1 << 31));
    }

    /**
     * Simulation for what will happen when writing an unsigned long value
     * as varlong.
     * @param value to consider
     * @return the number of bytes needed to write value.
     * @throws IOException
     */
    public static long sizeOfUnsignedVarLong(long value) throws IOException {
        int result = 0;
        do {
            result++;
            value >>>= 7;
        } while (value != 0);
        return result;
    }

    /**
     * Simulation for what will happen when writing a signed long value
     * as varlong.
     * @param value to consider
     * @return the number of bytes needed to write value.
     * @throws IOException
     */
    public static long sizeOfSignedVarLong(long value) throws IOException {
        return sizeOfUnsignedVarLong((value << 1) ^ (value >> 63));
    }

    /**
     * Simulation for what will happen when writing an unsigned int value
     * as varint.
     * @param value to consider
     * @return the number of bytes needed to write value.
     * @throws IOException
     */
    public static int sizeOfUnsignedVarInt(int value) throws IOException {
        int cnt = 0;
        do {
            cnt++;
            value >>>= 7;
        } while (value != 0);
        return cnt;
    }

    /**
     * Simulation for what will happen when writing a signed int value
     * as varint.
     * @param value to consider
     * @return the number of bytes needed to write value.
     * @throws IOException
     */
    public static int sizeOfSignedVarInt(int value) throws IOException {
        return sizeOfUnsignedVarInt((value << 1) ^ (value >> 31));
    }

}
