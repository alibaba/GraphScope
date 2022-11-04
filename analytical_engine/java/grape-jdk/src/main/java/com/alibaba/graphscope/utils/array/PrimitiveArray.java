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

package com.alibaba.graphscope.utils.array;

import com.alibaba.graphscope.ds.PrimitiveTypedArray;
import com.alibaba.graphscope.utils.array.impl.DoubleArray;
import com.alibaba.graphscope.utils.array.impl.IntArray;
import com.alibaba.graphscope.utils.array.impl.LongArray;
import com.alibaba.graphscope.utils.array.impl.ObjectArray;
import com.alibaba.graphscope.utils.array.impl.TypedBackendPrimitiveArray;

import java.io.Serializable;

public interface PrimitiveArray<T> extends Serializable {

    T get(int index);

    default T get(long index) {
        return get((int) index);
    }

    void set(int index, T value);

    default void set(long index, T value) {
        set((int) index, value);
    }

    int size();

    static <TT> PrimitiveArray<TT> create(Class<? extends TT> clz, int len) {
        if (clz.equals(double.class) || clz.equals(Double.class)) {
            return (PrimitiveArray<TT>) new DoubleArray(len);
        } else if (clz.equals(long.class) || clz.equals(Long.class)) {
            return (PrimitiveArray<TT>) new LongArray(len);
        } else if (clz.equals(int.class) || clz.equals(Integer.class)) {
            return (PrimitiveArray<TT>) new IntArray(len);
        } else {
            return (PrimitiveArray<TT>) new ObjectArray(clz, len);
        }
    }

    static <TT> PrimitiveArray<TT> createImmutable(
            PrimitiveTypedArray<TT> arrray, Class<? extends TT> clz) {
        return new TypedBackendPrimitiveArray<>(arrray);
    }
}
