/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.example.giraph;

// import org.apache.giraph.aggregators.DoubleMaxAggregator;
// import org.apache.giraph.aggregators.DoubleMinAggregator;
// import org.apache.giraph.aggregators.LongSumAggregator;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class PageRank
        extends BasicComputation<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

    /**
     * Number of supersteps for this test
     */
    public static final int MAX_SUPERSTEPS = 5;
    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(PageRank.class);
    /**
     * Sum aggregator name
     */
    private static String SUM_AGG = "sum";
    /**
     * Min aggregator name
     */
    private static String MIN_AGG = "min";
    /**
     * Max aggregator name
     */
    private static String MAX_AGG = "max";

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
            Iterable<DoubleWritable> messages)
            throws IOException {
        if (getSuperstep() >= 1) {
            double sum = 0;
            for (DoubleWritable message : messages) {
                LOG.debug("vertex: " + vertex.getId() + " Receive msg: " + message.get());
                sum += message.get();
            }
            DoubleWritable vertexValue =
                    new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
            vertex.setValue(vertexValue);
            aggregate(MAX_AGG, vertexValue);
            aggregate(MIN_AGG, vertexValue);
            aggregate(SUM_AGG, new LongWritable(1));
        }

        if (getSuperstep() < MAX_SUPERSTEPS) {
            long edges = vertex.getNumEdges();
            if (edges > 0) {
                sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() / edges));
                LOG.debug(
                        "vertex: "
                                + vertex.getId()
                                + "set to other vertices num: "
                                + edges
                                + " value: "
                                + vertex.getValue());
            } else {
                sendMessageToAllEdges(vertex, new DoubleWritable(1.0 / getTotalNumVertices()));
                LOG.debug(
                        "vertex: "
                                + vertex.getId()
                                + "set to other vertices num: "
                                + edges
                                + " base: "
                                + 1.0 / getTotalNumVertices());
            }

        } else {
            vertex.voteToHalt();
        }
    }

    /**
     * Worker context used with {@link PageRank}.
     */
    public static class SimplePageRankWorkerContext extends WorkerContext {

        /**
         * Final max value for verification for local jobs
         */
        private static double FINAL_MAX;
        /**
         * Final min value for verification for local jobs
         */
        private static double FINAL_MIN;
        /**
         * Final sum value for verification for local jobs
         */
        private static long FINAL_SUM;

        public static double getFinalMax() {
            return FINAL_MAX;
        }

        public static double getFinalMin() {
            return FINAL_MIN;
        }

        public static long getFinalSum() {
            return FINAL_SUM;
        }

        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {}

        @Override
        public void postApplication() {
            FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
            FINAL_MAX = this.<DoubleWritable>getAggregatedValue(MAX_AGG).get();
            FINAL_MIN = this.<DoubleWritable>getAggregatedValue(MIN_AGG).get();

            LOG.info("aggregatedNumVertices=" + FINAL_SUM);
            LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
            LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
        }

        @Override
        public void preSuperstep() {
            if (getSuperstep() >= 3) {
                LOG.info(
                        "aggregatedNumVertices="
                                + getAggregatedValue(SUM_AGG)
                                + " NumVertices="
                                + getTotalNumVertices());
                if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() != getTotalNumVertices()) {
                    throw new RuntimeException(
                            "wrong value of SumAggreg: "
                                    + getAggregatedValue(SUM_AGG)
                                    + ", should be: "
                                    + getTotalNumVertices());
                }
                DoubleWritable maxPagerank = getAggregatedValue(MAX_AGG);
                LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
                DoubleWritable minPagerank = getAggregatedValue(MIN_AGG);
                LOG.info("aggregatedMinPageRank=" + minPagerank.get());
            }
        }

        @Override
        public void postSuperstep() {}
    }

    /**
     * Master compute associated with {@link PageRank}. It registers required aggregators.
     */
    public static class SimplePageRankMasterCompute extends DefaultMasterCompute {

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_AGG, LongSumAggregator.class);
            registerPersistentAggregator(MIN_AGG, DoubleMinAggregator.class);
            registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
        }
    }

    /**
     * Simple VertexOutputFormat that supports {@link PageRank}
     */
    public static class SimplePageRankVertexOutputFormat
            extends TextVertexOutputFormat<LongWritable, DoubleWritable, DoubleWritable> {

        @Override
        public TextVertexWriter createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new SimplePageRankVertexWriter();
        }

        /**
         * Simple VertexWriter that supports {@link PageRank}
         */
        public class SimplePageRankVertexWriter extends TextVertexWriter {

            @Override
            public void writeVertex(Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex)
                    throws IOException, InterruptedException {
                getRecordWriter()
                        .write(
                                new Text(vertex.getId().toString()),
                                new Text(vertex.getValue().toString()));
            }
        }
    }
}
