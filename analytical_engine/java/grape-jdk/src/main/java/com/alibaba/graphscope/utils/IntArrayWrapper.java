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

public class IntArrayWrapper {
    private int data[];
    private int left;
    private int right;
    private int size;

    public IntArrayWrapper() {
        left = 0;
        right = 0;
        size = 0;
    }

    public IntArrayWrapper(int s, int defaultValue) {
        size = s;
        left = 0;
        right = s;
        data = new int[s];
        Arrays.fill(data, defaultValue);
    }

    public IntArrayWrapper(VertexRange<Long> vertices, int defaultValue) {
        left = vertices.beginValue().intValue();
        right = vertices.endValue().intValue();
        size = right - left;
        data = new int[size];
        Arrays.fill(data, defaultValue);
    }

    public int get(int ind) {
        return data[ind - left];
    }

    public int get(long ind) {
        return data[(int) ind - left];
    }

    public int get(Vertex<Long> vertex) {
        return data[vertex.getValue().intValue() - left];
    }

    public void set(long ind, int newValue) {
        data[(int) ind - left] = newValue;
    }

    public void set(int ind, int newValue) {
        data[ind - left] = newValue;
    }

    public void set(Vertex<Long> vertex, int newValue) {
        data[vertex.getValue().intValue() - left] = newValue;
    }

    public void set(int newValue) {
        for (int i = 0; i < size; ++i) {
            data[i] = newValue;
        }
    }

    public int getSize() {
        return size;
    }
}
