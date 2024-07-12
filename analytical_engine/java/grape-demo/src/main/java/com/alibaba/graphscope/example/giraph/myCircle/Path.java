package com.alibaba.graphscope.example.giraph.myCircle;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Path implements Writable {
    private List<Long> vertexIds;

    public Path() {
        vertexIds = new ArrayList<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
