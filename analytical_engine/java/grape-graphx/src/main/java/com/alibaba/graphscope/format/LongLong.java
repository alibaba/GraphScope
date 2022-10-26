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

package com.alibaba.graphscope.format;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongLong implements Writable {
    public long first;
    public long second;

    @Override
    public String toString() {
        return "LongLong{" + "first=" + first + ", second=" + second + '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(first);
        dataOutput.writeLong(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readLong();
        second = dataInput.readLong();
    }
}
