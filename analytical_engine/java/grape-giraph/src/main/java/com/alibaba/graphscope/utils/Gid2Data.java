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

public interface Gid2Data {

    static Gid2Data newResizable(int capacity) {
        return (Gid2Data) new Gid2DataResizable(capacity);
    }

    static Gid2Data newFixed(int capacity) {
        return (Gid2Data) new Gid2DataFixed(capacity);
    }

    boolean add(long gid, Writable writable);

    void clear();

    int size();

    /**
     * Number of bytes need for serialization.
     *
     * @return number of butes
     */
    int serializedSize();
}
