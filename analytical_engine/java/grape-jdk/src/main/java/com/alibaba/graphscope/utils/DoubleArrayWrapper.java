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
import com.google.common.util.concurrent.AtomicDouble;

public class DoubleArrayWrapper {
    private double data[];
    private int size;

    public DoubleArrayWrapper() {}

    public DoubleArrayWrapper(int s, double defaultValue) {
        size = s;
        data = new double[s];
        for (int i = 0; i < size; ++i) {
            data[i] = defaultValue;
        }
    }

    public double get(int ind) {
        return data[ind];
    }

    public double get(long ind) {
        return data[(int) ind];
    }

    public double get(Vertex<Long> vertex) {
        return data[vertex.GetValue().intValue()];
    }

    public void set(int ind, double newValue) {
        data[ind] = newValue;
    }

    public void set(long ind, double newValue) {
        data[(int) ind] = newValue;
    }

    public void set(Vertex<Long> vertex, AtomicDouble adouble) {
        data[vertex.GetValue().intValue()] = adouble.get();
    }

    public void set(Vertex<Long> vertex, double newValue) {
        data[vertex.GetValue().intValue()] = newValue;
    }

    public void set(double newValue) {
        for (int i = 0; i < size; ++i) {
            data[i] = newValue;
        }
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        set(Double.MAX_VALUE);
    }
}
