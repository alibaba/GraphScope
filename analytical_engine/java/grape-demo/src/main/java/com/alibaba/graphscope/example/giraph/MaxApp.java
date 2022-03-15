package com.alibaba.graphscope.example.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MaxApp
        extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(MaxApp.class);

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep. Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(
            Vertex<LongWritable, LongWritable, DoubleWritable> vertex,
            Iterable<LongWritable> messages)
            throws IOException {
        boolean changed = false;
        for (LongWritable message : messages) {
            if (vertex.getValue().get() < message.get()) {
                vertex.setValue(message);
                changed = true;
            }
        }
        if (getSuperstep() == 0 || changed) {
            sendMessageToAllEdges(vertex, vertex.getValue());
        }
        vertex.voteToHalt();
    }
}
