/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.interactive.client.utils;

public class Encoder {
    public Encoder(byte[] bs) {
        this.bs = bs;
        this.loc = 0;
    }

    public static int serialize_long(byte[] bytes, int offset, long value) {
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        return offset;
    }

    public static int serialize_double(byte[] bytes, int offset, double value) {
        long long_value = Double.doubleToRawLongBits(value);
        return serialize_long(bytes, offset, long_value);
    }

    public static int serialize_int(byte[] bytes, int offset, int value) {
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        return offset;
    }

    public static int serialize_byte(byte[] bytes, int offset, byte value) {
        bytes[offset++] = value;
        return offset;
    }

    public static int serialize_bytes(byte[] bytes, int offset, byte[] value) {
        offset = serialize_int(bytes, offset, value.length);
        System.arraycopy(value, 0, bytes, offset, value.length);
        return offset + value.length;
    }

    public void put_int(int value) {
        this.loc = serialize_int(this.bs, this.loc, value);
    }

    public void put_byte(byte value) {
        this.loc = serialize_byte(this.bs, this.loc, value);
    }

    public void put_long(long value) {
        this.loc = serialize_long(this.bs, this.loc, value);
    }

    public void put_double(double value) {
        this.loc = serialize_double(this.bs, this.loc, value);
    }

    public void put_bytes(byte[] bytes) {
        this.loc = serialize_bytes(this.bs, this.loc, bytes);
    }

    public void put_string(String value) {
        byte[] bytes = value.getBytes();
        this.put_bytes(bytes);
    }

    byte[] bs;
    int loc;
}
