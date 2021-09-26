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
import com.google.common.util.concurrent.AtomicDoubleArray;

public class AtomicDoubleArrayWrapper {
    private AtomicDoubleArray data;
    private int size;

    public AtomicDoubleArrayWrapper() {}

    public AtomicDoubleArrayWrapper(int s, double defaultValue) {
        size = s;
        data = new AtomicDoubleArray(s);
        for (int i = 0; i < size; ++i) {
            data.set(i, defaultValue);
        }
    }

    public double get(int ind) {
        return data.get(ind);
    }

    public double get(long ind) {
        return data.get((int) ind);
    }

    public double get(Vertex<Long> vertex) {
        return data.get(vertex.GetValue().intValue());
    }

    public void set(int ind, double newValue) {
        data.set(ind, newValue);
    }

    public void set(long ind, double newValue) {
        data.set((int) ind, newValue);
    }

    public void set(Vertex<Long> vertex, double newValue) {
        int lid = vertex.GetValue().intValue();
        data.set(lid, newValue);
    }

    public void set(double newValue) {
        for (int i = 0; i < size; ++i) {
            data.set(i, newValue);
        }
    }

    /*
     * we want to set the smaller one to ind.
     */
    public void compareAndSetMin(int ind, double newValue) {
        double preValue;
        do {
            preValue = data.get(ind);
        } while (preValue > newValue && !data.compareAndSet(ind, preValue, newValue));
    }

    public void compareAndSetMin(long ind, double newValue) {
        double preValue;
        do {
            preValue = data.get((int) ind);
        } while (preValue > newValue && !data.compareAndSet((int) ind, preValue, newValue));
    }

    public void compareAndSetMin(Vertex<Long> vertex, double newValue) {
        int lid = vertex.GetValue().intValue();
        double preValue;
        do {
            preValue = data.get(lid);
        } while (preValue > newValue && !data.compareAndSet(lid, preValue, newValue));
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        set(0.0);
    }
}
