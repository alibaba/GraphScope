package com.alibaba.graphscope.example.giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Traverse
        extends BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(Traverse.class);
    private static long edgesCnt = 0;
    private static long MAX_SUPER_STEP = 10;

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
        if (getSuperstep() >= MAX_SUPER_STEP) {
            vertex.voteToHalt();
            return;
        }
        for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
            //            LOG.info("src {}, dst {}, value {}", vertex.getId(),
            // edge.getTargetVertexId(), edge.getValue());
            edgesCnt += 1;
        }
        vertex.wakeUp();
    }

    public static class TraverseContext extends WorkerContext {

        private static Logger LOG = LoggerFactory.getLogger(TraverseContext.class);

        /**
         * Initialize the WorkerContext. This method is executed once on each Worker before the
         * first superstep starts.
         *
         * @throws IllegalAccessException Thrown for getting the class
         * @throws InstantiationException Expected instantiation in this method.
         */
        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {
            LOG.info("PreApplication");
        }

        /**
         * Finalize the WorkerContext. This method is executed once on each Worker after the last
         * superstep ends.
         */
        @Override
        public void postApplication() {
            LOG.info("PostApplication");
            LOG.info("Total edges traversed : [{}]", edgesCnt);
        }

        /**
         * Execute user code. This method is executed once on each Worker before each superstep
         * starts.
         */
        @Override
        public void preSuperstep() {
            LOG.info("PreSuperstep : " + getSuperstep());
        }

        /**
         * Execute user code. This method is executed once on each Worker after each superstep
         * ends.
         */
        @Override
        public void postSuperstep() {
            LOG.info("PostSuperstep: " + getSuperstep());
        }
    }
}
