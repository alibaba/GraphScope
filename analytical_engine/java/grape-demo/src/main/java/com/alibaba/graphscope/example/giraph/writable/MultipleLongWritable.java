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
