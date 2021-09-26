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

package org.apache.giraph.examples;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Vertex to allow unit testing of graph mutations.
 */
public class SimpleMutateGraphComputation
        extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /**
     * Class logger
     */
    private static Logger LOG = LoggerFactory.getLogger(SimpleMutateGraphComputation.class);
    /**
     * Maximum number of ranges for vertex ids
     */
    private long maxRanges = 100;

    /**
     * Unless we create a ridiculous number of vertices , we should not collide within a vertex
     * range defined by this method.
     *
     * @param range Range index
     * @return Starting vertex id of the range
     */
    private long rangeVertexIdStart(int range) {
        return (Long.MAX_VALUE / maxRanges) * range;
    }

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages)
            throws IOException {
        SimpleMutateGraphVertexWorkerContext workerContext =
                (SimpleMutateGraphVertexWorkerContext) getWorkerContext();
        if (getSuperstep() == 0) {
            LOG.debug("Reached superstep " + getSuperstep());
        } else if (getSuperstep() == 1) {
            // Send messages to vertices that are sure not to exist
            // (creating them)
            LongWritable destVertexId =
                    new LongWritable(rangeVertexIdStart(1) + vertex.getId().get());
            sendMessage(destVertexId, new DoubleWritable(0.0));
        } else if (getSuperstep() == 2) {
            LOG.debug("Reached superstep " + getSuperstep());
        } else if (getSuperstep() == 3) {
            long vertexCount = workerContext.getVertexCount();
            if (vertexCount * 2 != getTotalNumVertices()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumVertices()
                                + " vertices when should have "
                                + vertexCount * 2
                                + " on superstep "
                                + getSuperstep());
            }
            long edgeCount = workerContext.getEdgeCount();
            if (edgeCount != getTotalNumEdges()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumEdges()
                                + " edges when should have "
                                + edgeCount
                                + " on superstep "
                                + getSuperstep());
            }
            // Create vertices that are sure not to exist (doubling vertices)
            LongWritable vertexIndex =
                    new LongWritable(rangeVertexIdStart(3) + vertex.getId().get());
            addVertexRequest(vertexIndex, new DoubleWritable(0.0));
            // Add edges to those remote vertices as well
            addEdgeRequest(
                    vertexIndex, EdgeFactory.create(vertex.getId(), new FloatWritable(0.0f)));
        } else if (getSuperstep() == 4) {
            LOG.debug("Reached superstep " + getSuperstep());
        } else if (getSuperstep() == 5) {
            long vertexCount = workerContext.getVertexCount();
            if (vertexCount * 2 != getTotalNumVertices()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumVertices()
                                + " when should have "
                                + vertexCount * 2
                                + " on superstep "
                                + getSuperstep());
            }
            long edgeCount = workerContext.getEdgeCount();
            if (edgeCount + vertexCount != getTotalNumEdges()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumEdges()
                                + " edges when should have "
                                + edgeCount
                                + vertexCount
                                + " on superstep "
                                + getSuperstep());
            }
            // Remove the edges created in superstep 3
            LongWritable vertexIndex =
                    new LongWritable(rangeVertexIdStart(3) + vertex.getId().get());
            workerContext.increaseEdgesRemoved();
            removeEdgesRequest(vertexIndex, vertex.getId());
        } else if (getSuperstep() == 6) {
            // Remove all the vertices created in superstep 3
            if (vertex.getId().compareTo(new LongWritable(rangeVertexIdStart(3))) >= 0) {
                removeVertexRequest(vertex.getId());
            }
        } else if (getSuperstep() == 7) {
            long origEdgeCount = workerContext.getOrigEdgeCount();
            if (origEdgeCount != getTotalNumEdges()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumEdges()
                                + " edges when should have "
                                + origEdgeCount
                                + " on superstep "
                                + getSuperstep());
            }
        } else if (getSuperstep() == 8) {
            long vertexCount = workerContext.getVertexCount();
            if (vertexCount / 2 != getTotalNumVertices()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumVertices()
                                + " vertices when should have "
                                + vertexCount / 2
                                + " on superstep "
                                + getSuperstep());
            }
        } else if (getSuperstep() == 9) {
            // Remove all the vertices created in superstep 1, and send a message to
            // them at the same time
            if (vertex.getId().compareTo(new LongWritable(rangeVertexIdStart(1))) >= 0) {
                // This is an added vertex, so remove it
                removeVertexRequest(vertex.getId());
            } else {
                // This is a vertex since the start of the computation, so send a
                // message to a vertex added in superstep 1
                sendMessage(
                        new LongWritable(rangeVertexIdStart(1) + vertex.getId().get()),
                        new DoubleWritable(0.0));
            }
        } else if (getSuperstep() == 10) {
            LOG.debug("Reached superstep " + getSuperstep());
        } else if (getSuperstep() == 11) {
            long vertexCount = workerContext.getVertexCount();
            if (vertexCount / 2 != getTotalNumVertices()) {
                throw new IllegalStateException(
                        "Impossible to have "
                                + getTotalNumVertices()
                                + " vertices when should have "
                                + vertexCount / 2
                                + " on superstep "
                                + getSuperstep());
            }
        } else {
            vertex.voteToHalt();
        }
    }

    /**
     * Worker context used with {@link SimpleMutateGraphComputation}.
     */
    public static class SimpleMutateGraphVertexWorkerContext extends WorkerContext {

        /**
         * Cached vertex count
         */
        private long vertexCount;
        /**
         * Cached edge count
         */
        private long edgeCount;
        /**
         * Original number of edges
         */
        private long origEdgeCount;
        /**
         * Number of edges removed during superstep
         */
        private int edgesRemoved = 0;

        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {}

        @Override
        public void postApplication() {}

        @Override
        public void preSuperstep() {}

        @Override
        public void postSuperstep() {
            vertexCount = getTotalNumVertices();
            edgeCount = getTotalNumEdges();
            if (getSuperstep() == 1) {
                origEdgeCount = edgeCount;
            }
            LOG.info(
                    "Got "
                            + vertexCount
                            + " vertices, "
                            + edgeCount
                            + " edges on superstep "
                            + getSuperstep());
            LOG.info("Removed " + edgesRemoved);
            edgesRemoved = 0;
        }

        public long getVertexCount() {
            return vertexCount;
        }

        public long getEdgeCount() {
            return edgeCount;
        }

        public long getOrigEdgeCount() {
            return origEdgeCount;
        }

        /**
         * Increase the number of edges removed by one.
         */
        public void increaseEdgesRemoved() {
            this.edgesRemoved++;
        }
    }
}
