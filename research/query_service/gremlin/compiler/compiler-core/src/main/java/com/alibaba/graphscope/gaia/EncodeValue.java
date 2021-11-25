/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.gaia;

import com.alibaba.graphscope.common.proto.Common;
import com.google.protobuf.ByteString;

public final class EncodeValue {
    public static Common.Value fromBool(final boolean v) {
        return Common.Value.newBuilder()
                .setBoolean(v)
                .build();
    }

    public static Common.Value fromInt(final int v) {
        return Common.Value.newBuilder()
                .setI32(v)
                .build();
    }

    public static Common.Value fromLong(final long v) {
        return Common.Value.newBuilder()
                .setI64(v)
                .build();
    }

    public static Common.Value fromString(final String v) {
        return Common.Value.newBuilder()
                .setStr(v)
                .build();
    }

    public static Common.Value fromBytes(final byte[] bytes) {
        return Common.Value.newBuilder()
                .setBlob(ByteString.copyFrom(bytes))
                .build();
    }

    public static Common.Value fromDouble(final double v) {
        return Common.Value.newBuilder()
                .setF64(v)
                .build();
    }

    public static Common.Value fromIntArray(final int[] array) {
        Common.I32Array.Builder b = Common.I32Array.newBuilder();
        for (int j : array) {
            b.addItem(j);
        }
        return Common.Value.newBuilder()
                .setI32Array(b)
                .build();
    }

    public static Common.Value fromIntArray(final Iterable<Integer> array) {
        Common.I32Array v = Common.I32Array.newBuilder().addAllItem(array).build();
        return Common.Value.newBuilder()
                .setI32Array(v)
                .build();
    }

    public static Common.Value fromLongArray(final long[] array) {
        Common.I64Array.Builder b = Common.I64Array.newBuilder();
        for (long j : array) {
            b.addItem(j);
        }
        return Common.Value.newBuilder()
                .setI64Array(b)
                .build();
    }

    public static Common.Value fromLongArray(final Iterable<Long> array) {
        Common.I64Array.Builder b = Common.I64Array.newBuilder().addAllItem(array);
        return Common.Value.newBuilder()
                .setI64Array(b)
                .build();
    }

    public static Common.Value fromDoubleArray(double[] array) {
        Common.DoubleArray.Builder b = Common.DoubleArray.newBuilder();
        for (double j : array) {
            b.addItem(j);
        }
        return Common.Value.newBuilder()
                .setF64Array(b)
                .build();
    }

    public static Common.Value fromDoubleArray(Iterable<Double> array) {
        Common.DoubleArray.Builder b = Common.DoubleArray.newBuilder().addAllItem(array);
        return Common.Value.newBuilder()
                .setF64Array(b)
                .build();
    }

    public static Common.Value fromStrArray(Iterable<String> array) {
        Common.StringArray.Builder b = Common.StringArray.newBuilder().addAllItem(array);
        return Common.Value.newBuilder()
                .setStrArray(b)
                .build();
    }

    public static Common.Value fromNull() {
        return Common.Value.newBuilder()
                .setNone(Common.None.newBuilder().build())
                .build();
    }
}

