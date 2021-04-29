package com.alibaba.maxgraph.databuild;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataBuildReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    @Override
    protected void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        for (BytesWritable value : values) {
            context.write(key, value);
        }
    }

}
