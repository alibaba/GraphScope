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

import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(name = "Shortest paths", description = "Finds all shortest paths from a selected vertex")
public class SSSP extends BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(SSSP.class);
    /**
     * The shortest paths id
     */
    public static LongConfOption SOURCE_ID;

    static {
        String ssspSource = System.getenv("SSSP_SOURCE");
        if (Objects.isNull(ssspSource) || ssspSource.isEmpty()) {
            SOURCE_ID = new LongConfOption("sourceId", 1, "The shortest paths id");
        } else {
            SOURCE_ID =
                    new LongConfOption(
                            "sourceId", Long.valueOf(ssspSource), "The shortest paths id");
        }
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

    @Override
    public void compute(
            Vertex<LongWritable, LongWritable, LongWritable> vertex,
            Iterable<LongWritable> messages)
            throws IOException {
        if (getSuperstep() == 0) {
            vertex.setValue(new LongWritable(Long.MAX_VALUE));
        }
        long minDist = isSource(vertex) ? 0L : Long.MAX_VALUE;
        for (LongWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (minDist < vertex.getValue().get()) {
            vertex.setValue(new LongWritable(minDist));
            for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
                long distance = minDist + edge.getValue().get();
                sendMessage(edge.getTargetVertexId(), new LongWritable(distance));
            }
        }
        vertex.voteToHalt();
    }

    public static class SSSPWorkerContext extends WorkerContext {

        private static final Logger LOG = LoggerFactory.getLogger(SSSPWorkerContext.class);

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

    /**
     * Simple VertexOutputFormat.
     */
    public static class SimpleSuperstepVertexOutputFormat
            extends TextVertexOutputFormat<LongWritable, LongWritable, LongWritable> {

        /**
         * The factory method which produces the {@link TextVertexWriter} used by this output
         * format.
         *
         * @param context the information about the task
         * @return the text vertex writer to be used
         */
        @Override
        public TextVertexWriter createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new SimpleSuperstepVertexWriter();
        }

        /**
         * Simple VertexWriter.
         */
        public class SimpleSuperstepVertexWriter extends TextVertexWriter {

            @Override
            public void writeVertex(Vertex<LongWritable, LongWritable, LongWritable> vertex)
                    throws IOException, InterruptedException {
                getRecordWriter()
                        .write(
                                new Text(vertex.getId().toString()),
                                new Text(vertex.getValue().toString()));
            }
        }
    }
}
