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

package com.alibaba.graphscope.utils.array.impl;

import com.alibaba.graphscope.utils.array.PrimitiveArray;

public class LongArray implements PrimitiveArray<Long> {
    private long[] values;

    public LongArray(int len) {
        values = new long[len];
    }

    @Override
    public Long get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Long value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }
}
