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

import org.apache.hadoop.io.Writable;

import java.util.ArrayList;

public class Gid2DataResizable implements Gid2Data {

    private int size;
    private ArrayList<Long> gids;
    private ArrayList<Writable> data;

    /**
     * Not resizable.
     *
     * @param capacity
     */
    public Gid2DataResizable(int capacity) {
        gids = new ArrayList<>(capacity);
        data = new ArrayList<>(capacity);
        size = 0;
    }

    public ArrayList<Long> getGids() {
        return gids;
    }

    public ArrayList<Writable> getData() {
        return data;
    }

    public boolean add(long gid, Writable writable) {
        gids.add(gid);
        data.add(writable);
        size += 1;
        return true;
    }

    public void clear() {
        gids.clear();
        data.clear();
        size = 0;
    }

    public int size() {
        return size;
    }

    /**
     * Number of bytes need for serialization.
     *
     * @return number of butes
     */
    @Override
    public int serializedSize() {
        throw new IllegalStateException("Not implemented intentionally");
    }
}
