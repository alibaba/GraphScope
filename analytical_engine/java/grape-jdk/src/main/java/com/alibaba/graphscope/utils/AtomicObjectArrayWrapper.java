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

package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class AtomicObjectArrayWrapper<T> {

    private AtomicReferenceArray<T> data;
    private int size;

    public AtomicObjectArrayWrapper(int s) {
        data = new AtomicReferenceArray<>(s);
        size = s;
    }

    public T get(int ind) {
        return data.get(ind);
    }

    public T get(long ind) {
        return data.get((int) ind);
    }

    public T get(Vertex<Long> vertex) {
        return data.get(vertex.GetValue().intValue());
    }

    public void set(int ind, T newValue) {
        data.set(ind, newValue);
    }

    public void set(long ind, T newValue) {
        data.set((int) ind, newValue);
    }

    public void set(Vertex<Long> vertex, T newValue) {
        int lid = vertex.GetValue().intValue();
        data.set(lid, newValue);
    }

    public void set(T newValue) {
        for (int i = 0; i < size; ++i) {
            data.set(i, newValue);
        }
    }

    public void compareAndSet(int ind, T newValue) {
        T preValue;
        do {
            preValue = data.get(ind);
        } while (!preValue.equals(newValue) && !data.compareAndSet(ind, preValue, newValue));
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        set(null);
    }
}
