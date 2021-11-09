/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import java.util.Arrays;

public class LongArrayWrapper {
    private long data[];
    private int size;
    private int left;
    private int right;

    public LongArrayWrapper() {
        left = 0;
        right = 0;
        size = 0;
    }

    public LongArrayWrapper(int s, long defaultValue) {
        size = s;
        left = 0;
        right = s;
        data = new long[s];
        Arrays.fill(data, defaultValue);
    }

    public LongArrayWrapper(VertexRange<Long> vertices, long defaultValue) {
        left = vertices.begin().GetValue().intValue();
        right = vertices.end().GetValue().intValue();
        size = right - left;
        data = new long[size];
        Arrays.fill(data, defaultValue);
    }

    public long get(Vertex<Long> vertex) {
        return data[vertex.GetValue().intValue() - left];
    }

    public long get(int ind) {
        return data[ind - left];
    }

    public long get(long ind) {
        return data[(int) ind - left];
    }

    public void set(Vertex<Long> vertex, long newValue) {
        data[vertex.GetValue().intValue() - left] = newValue;
    }

    public void set(long ind, long newValue) {
        data[(int) ind - left] = newValue;
    }

    public void set(int ind, long newValue) {
        data[ind - left] = newValue;
    }

    public void set(long newValue) {
        for (int i = 0; i < size; ++i) {
            data[i] = newValue;
        }
    }

    public int getSize() {
        return size;
    }
}
