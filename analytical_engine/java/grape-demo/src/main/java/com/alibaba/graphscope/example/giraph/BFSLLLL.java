package com.alibaba.graphscope.example.giraph;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class BFSLLLL
        extends BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

    /**
     * The start id
     */
    public static LongConfOption SOURCE_ID;

    private static Logger logger = LoggerFactory.getLogger(BFSLLLL.class);

    static {
        String bfsSource = System.getenv("BFS_SOURCE");
        if (Objects.isNull(bfsSource) || bfsSource.isEmpty()) {
            SOURCE_ID = new LongConfOption("bfs.sourceId", 1, "The shortest paths id");
        } else {
            SOURCE_ID =
                    new LongConfOption(
                            "bfs.sourceId", Long.valueOf(bfsSource), "The shortest paths id");
        }
    }

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep. Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(
            Vertex<LongWritable, LongWritable, LongWritable> vertex,
            Iterable<LongWritable> messages)
            throws IOException {
        boolean changed = false;
        if (getSuperstep() == 0) {
            if (isSource(vertex)) {
                vertex.setValue(new LongWritable(0));
                changed = true;
            } else {
                vertex.setValue(new LongWritable(Integer.MAX_VALUE));
            }
        }

        for (LongWritable message : messages) {
            if (vertex.getValue().get() > message.get()) {
                logger.debug("v: {}, prev {}, msg {}", vertex.getId(), vertex.getValue(), message);
                vertex.setValue(message);
                changed = true;
            }
        }
        if (changed) {
            sendMessageToAllEdges(vertex, new LongWritable(vertex.getValue().get() + 1));
            logger.debug("vertex {} sending msg to all edges", vertex.getId());
        }
        vertex.voteToHalt();
    }

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }
}
