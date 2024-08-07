package com.alibaba.graphscope.example.giraph.myCircle;

import com.alibaba.graphscope.example.giraph.circle.VertexAttrWritable;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Circle extends BasicComputation<LongWritable, VertexAttrWritable, LongWritable, VertexAttrWritable> {
    private static final Logger logger = LoggerFactory.getLogger(com.alibaba.graphscope.example.giraph.myCircle.Circle.class);
    int maxIteration = 3;

    public Circle() {
    }

    public void preSuperstep() {
        this.maxIteration = Integer.parseInt(this.getConf().get("max", "3"));
        logger.info("[preSuperstep] max is {}", this.maxIteration);
    }
    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous
     *                 superstep.  Each message is only guaranteed to have
     */
    @Override
    public void compute(Vertex<LongWritable, VertexAttrWritable, LongWritable> vertex, Iterable<VertexAttrWritable> messages) throws IOException {

    }
}
