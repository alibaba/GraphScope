package com.alibaba.graphscope.example.giraph;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AggregateBenchmark
        extends BasicComputation<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

    /**
     * Number of supersteps for this test
     */
    // public static final int MAX_SUPERSTEPS = 500;
    public static final int MAX_SUPERSTEPS = 5;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(AggregateBenchmark.class);
    /**
     * Sum aggregator name
     */
    private static String SUM_AGG = "sum";

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep. Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
            Iterable<DoubleWritable> messages)
            throws IOException {
        if (vertex.getId().get() < 10) {
            aggregate(SUM_AGG, new LongWritable(1));
        }

        if (getSuperstep() >= MAX_SUPERSTEPS) {
            vertex.voteToHalt();
        } else {
            vertex.wakeUp();
        }
    }

    /**
     * Master compute associated with {@link PageRank}. It registers required aggregators.
     */
    public static class AggregateBenchmarkMasterCompute extends DefaultMasterCompute {

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_AGG, LongSumAggregator.class);
        }
    }

    /**
     * Worker context used with {@link PageRank}.
     */
    public static class AggregateBenchmarkWorkerContext extends WorkerContext {

        /**
         * Final sum value for verification for local jobs
         */
        private static long FINAL_SUM;

        public static long getFinalSum() {
            return FINAL_SUM;
        }

        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {}

        @Override
        public void postApplication() {
            FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
            LOG.info("aggregatedNumVertices=" + FINAL_SUM);
        }

        @Override
        public void preSuperstep() {
            if (getSuperstep() >= 1) {
                LOG.info(
                        "aggregatedNumVertices="
                                + getAggregatedValue(SUM_AGG)
                                + " NumVertices="
                                + getTotalNumVertices());
            }
        }

        @Override
        public void postSuperstep() {}
    }
}
