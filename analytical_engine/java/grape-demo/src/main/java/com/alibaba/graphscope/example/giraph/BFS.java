package com.alibaba.graphscope.example.giraph;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BFS extends BasicComputation<IntWritable, IntWritable, FloatWritable, IntWritable> {

    /**
     * The start id
     */
    public static final LongConfOption SOURCE_ID =
            new LongConfOption("bfs.sourceId", 6, "The shortest paths id");

    private static Logger logger = LoggerFactory.getLogger(BFS.class);

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep. Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(
            Vertex<IntWritable, IntWritable, FloatWritable> vertex, Iterable<IntWritable> messages)
            throws IOException {
        boolean changed = false;
        if (getSuperstep() == 0) {
            if (isSource(vertex)) {
                vertex.setValue(new IntWritable(0));
                changed = true;
            } else {
                vertex.setValue(new IntWritable(Integer.MAX_VALUE));
            }
        }

        for (IntWritable message : messages) {
            if (vertex.getValue().get() > message.get()) {
                vertex.setValue(message);
                changed = true;
            }
        }
        if (changed) {
            sendMessageToAllEdges(vertex, new IntWritable(vertex.getValue().get() + 1));
        }
        vertex.voteToHalt();
    }

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<IntWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }
}
