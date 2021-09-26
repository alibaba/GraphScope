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

import java.io.DataOutput;
import java.io.IOException;

public class Gid2DataFixed implements Gid2Data {

    private int size;
    private long[] gids;
    private Writable[] data;

    /**
     * Not resizable.
     *
     * @param capacity
     */
    public Gid2DataFixed(int capacity) {
        gids = new long[capacity];
        data = new Writable[capacity];
        size = 0;
    }

    public long[] getGids() {
        return gids;
    }

    public Writable[] getMsgOnVertex() {
        return data;
    }

    public boolean add(long gid, Writable writable) {
        if (size == gids.length) {
            return false;
        } else {
            gids[size] = gid;
            data[size++] = writable;
            return true;
        }
    }

    public void clear() {
        // release objs
        for (int i = 0; i < size; ++i) {
            data[i] = null;
        }
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
        // minimum size of writable is 0.
        return size * (4 + 0);
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(size);
        for (int i = 0; i < size; ++i) {
            output.writeLong(gids[i]);
            data[i].write(output);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Gid2DataFix(size=" + size);
        sb.append(",gids=");
        sb.append(gids[0]).append("...").append(data[0]).append("...)");
        return sb.toString();
    }
}
