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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

public class AtomicLongArrayWrapper {
    private AtomicLongArray data;
    private int size;

    public AtomicLongArrayWrapper(int s, long defaultValue) {
        size = s;
        long tmp[] = new long[s];
        Arrays.fill(tmp, defaultValue);
        data = new AtomicLongArray(tmp);
    }

    public AtomicLongArrayWrapper(int s) {
        size = s;
        data = new AtomicLongArray(s);
    }

    public long get(int ind) {
        return data.get(ind);
    }

    public long get(long ind) {
        return data.get((int) ind);
    }

    public long get(Vertex<Long> vertex) {
        return data.get(vertex.getValue().intValue());
    }

    public void set(int ind, long newValue) {
        data.set(ind, newValue);
    }

    public void set(long ind, long newValue) {
        data.set((int) ind, newValue);
    }

    public void set(Vertex<Long> vertex, long newValue) {
        int lid = vertex.getValue().intValue();
        data.set(lid, newValue);
    }

    public void set(long newValue) {
        for (int i = 0; i < size; ++i) {
            data.set(i, newValue);
        }
    }

    /*
     * we want to set the smaller one to ind.
     */
    public void compareAndSetMin(int ind, long newValue) {
        long preValue;
        do {
            preValue = data.get(ind);
        } while (preValue > newValue && !data.compareAndSet(ind, preValue, newValue));
    }

    public void compareAndSetMin(long ind, long newValue) {
        long preValue;
        do {
            preValue = data.get((int) ind);
        } while (preValue > newValue && !data.compareAndSet((int) ind, preValue, newValue));
    }

    public void compareAndSetMin(Vertex<Long> vertex, long newValue) {
        int lid = vertex.getValue().intValue();
        long preValue;
        do {
            preValue = data.get(lid);
        } while (preValue > newValue && !data.compareAndSet(lid, preValue, newValue));
    }

    public void compareAndSet(int ind, long newValue) {
        long preValue;
        do {
            preValue = data.get(ind);
        } while (preValue != newValue && !data.compareAndSet(ind, preValue, newValue));
    }

    /**
     * Atomicl update the array, compare values using unsigned comparasion.
     *
     * @param vertex querying vertex.
     * @param newValue new value.
     */
    public void compareAndSetMinUnsigned(Vertex<Long> vertex, long newValue) {
        int lid = vertex.getValue().intValue();
        long preValue;
        do {
            preValue = data.get(lid);
        } while (Long.compareUnsigned(preValue, newValue) > 0
                && !data.compareAndSet(lid, preValue, newValue));
    }

    /**
     * Atomicl update the array, compare values using unsigned comparasion.
     *
     * @param vertexId querying vertex id.
     * @param newValue new value.
     */
    public void compareAndSetMinUnsigned(long vertexId, long newValue) {
        int vid = (int) vertexId;
        long preValue;
        do {
            preValue = data.get(vid);
        } while (Long.compareUnsigned(preValue, newValue) > 0
                && !data.compareAndSet(vid, preValue, newValue));
    }

    public int getSize() {
        return size;
    }
}
