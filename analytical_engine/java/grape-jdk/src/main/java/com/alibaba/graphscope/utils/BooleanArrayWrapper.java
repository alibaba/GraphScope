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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanArrayWrapper {
    private static Logger logger = LoggerFactory.getLogger(BooleanArrayWrapper.class.getName());

    private boolean data[];
    private int size;

    public BooleanArrayWrapper() {}

    public BooleanArrayWrapper(int s, boolean defaultValue) {
        size = s;
        data = new boolean[s];
        for (int i = 0; i < size; ++i) {
            data[i] = defaultValue;
        }
    }

    /**
     * Check if any ele in [start, end] is true
     *
     * @param start left boundary.
     * @param end right boundary.
     * @return true if empty in this range.
     */
    public boolean partialEmpty(int start, int end) {
        if (end >= size) {
            logger.error("Error: out of bound" + end + "," + size);
        }
        for (int i = start; i <= end; ++i) {
            if (data[i]) return false;
        }
        return true;
    }

    public boolean partialEmpty(long start, long end) {
        if (end >= size) {
            logger.error("Error: out of bound" + end + "," + size);
        }
        for (int i = (int) start; i <= end; ++i) {
            if (data[i]) return false;
        }
        return true;
    }

    public void assign(BooleanArrayWrapper other) {
        if (other.getSize() != size) {
            logger.error("cannot be assigned since size don't equal");
            return;
        }
        for (int i = 0; i < size; ++i) {
            data[i] = other.get(i);
        }
    }

    public boolean get(int ind) {
        return data[ind];
    }

    public boolean get(long ind) {
        return data[(int) ind];
    }

    public boolean get(Vertex<Long> vertex) {
        return data[vertex.getValue().intValue()];
    }

    public void set(int ind, boolean newValue) {
        data[ind] = newValue;
    }

    public void set(Vertex<Long> vertex, boolean newvalue) {
        data[vertex.getValue().intValue()] = newvalue;
    }

    public void set(long ind, boolean newValue) {
        data[(int) ind] = newValue;
    }

    public void set(boolean newValue) {
        for (int i = 0; i < size; ++i) {
            data[i] = newValue;
        }
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        for (int i = 0; i < size; ++i) {
            data[i] = false;
        }
    }
}
