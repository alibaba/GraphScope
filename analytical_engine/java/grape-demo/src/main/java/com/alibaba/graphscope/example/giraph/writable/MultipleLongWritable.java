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

package com.alibaba.graphscope.example.giraph.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MultipleLongWritable implements Writable {

    private LongWritable writable;
    private int repeatTimes;

    public MultipleLongWritable() {
        writable = new LongWritable(0);
        repeatTimes = 1;
    }

    public MultipleLongWritable(long value) {
        writable = new LongWritable(value);
        repeatTimes = 1;
    }

    public MultipleLongWritable(LongWritable data, int times) {
        this.writable = data;
        this.repeatTimes = times;
    }

    public long get() {
        return writable.get();
    }

    public void set(long value) {
        this.writable.set(value);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        repeatTimes = input.readInt();
        for (int i = 0; i < repeatTimes; ++i) {
            writable.readFields(input);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(repeatTimes);
        for (int i = 0; i < repeatTimes; ++i) {
            writable.write(output);
        }
    }

    @Override
    public String toString() {
        return "" + writable + "@time:" + repeatTimes;
    }
}
