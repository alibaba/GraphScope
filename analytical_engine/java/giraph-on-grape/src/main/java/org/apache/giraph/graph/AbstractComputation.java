/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.giraph.graph;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.worker.WorkerAggregatorDelegator;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Implement all methods in Computation other than compute, which left for user to define.
 *
 * @param <OID_T>     original vertex id.
 * @param <VDATA_T>   vertex data type.
 * @param <EDATA_T>   edata type.
 * @param <IN_MSG_T>  incoming msg type.
 * @param <OUT_MSG_T> outgoing msg type.
 */
public abstract class AbstractComputation<
                OID_T extends WritableComparable,
                VDATA_T extends Writable,
                EDATA_T extends Writable,
                IN_MSG_T extends Writable,
                OUT_MSG_T extends Writable>
        extends WorkerAggregatorDelegator<OID_T, VDATA_T, EDATA_T>
        implements Computation<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(AbstractComputation.class);

    /**
     * We hold a communicatorImpl rather that directly inherit from CommunicatorImpl. So
     * CommunicatorImpl can also be reference in workerContext.
     */
    //    private Communicator communicator;

    private GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, ?, ?>
            giraphMessageManager;

    private IFragment fragment;
    private int curStep = 0;
    private WorkerContext workerContext;
    //    private AggregatorManager aggregatorManager;

    public void setGiraphMessageManager(
            GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, ?, ?>
                    giraphMessageManager) {
        this.giraphMessageManager = giraphMessageManager;
    }

    public void setFragment(IFragment fragment) {
        this.fragment = fragment;
    }

    /**
     * Called by our framework after each super step.
     */
    public void incStep() {
        curStep++;
    }

    /**
     * Prepare for computation. This method is executed exactly once prior to {@link
     * #compute(Vertex, Iterable)} being called for any of the vertices in the partition.
     */
    @Override
    public void preSuperstep() {}

    /**
     * Finish computation. This method is executed exactly once after computation for all vertices
     * in the partition is complete.
     */
    @Override
    public void postSuperstep() {}

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    @Override
    public long getSuperstep() {
        return curStep;
    }

    /**
     * Get the total (all workers) number of vertices that existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    @Override
    public long getTotalNumVertices() {
        return fragment.getTotalVerticesNum();
    }

    /**
     * Get the total (all workers) number of edges that existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    @Override
    public long getTotalNumEdges() {
        return fragment.getEdgeNum();
    }

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    @Override
    public Context getContext() {
        return null;
    }

    /**
     * Get the worker context
     *
     * @return WorkerContext context
     */
    @Override
    public WorkerContext getWorkerContext() {
        return workerContext;
    }

    public void setWorkerContext(WorkerContext workerContext) {
        this.workerContext = workerContext;
    }

    @Override
    public void addVertexRequest(OID_T id, VDATA_T value, OutEdges<OID_T, EDATA_T> edges)
            throws IOException {
        logger.error("Not implemented");
    }

    @Override
    public void addVertexRequest(OID_T id, VDATA_T value) throws IOException {
        logger.error("Not implemented");
    }

    @Override
    public void removeVertexRequest(OID_T vertexId) throws IOException {
        logger.error("Not implemented");
    }

    @Override
    public void addEdgeRequest(OID_T sourceVertexId, Edge<OID_T, EDATA_T> edge) throws IOException {
        logger.error("Not implemented");
    }

    @Override
    public void removeEdgesRequest(OID_T sourceVertexId, OID_T targetVertexId) throws IOException {
        logger.error("Not implemented");
    }

    @Override
    public void sendMessage(OID_T id, OUT_MSG_T message) {
        giraphMessageManager.sendMessage(id, message);
    }

    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {
        giraphMessageManager.sendMessageToAllEdges(vertex, message);
    }

    @Override
    public void sendMessageToMultipleEdges(Iterator<OID_T> vertexIdIterator, OUT_MSG_T message) {}

    /**
     * Get number of workers
     *
     * @return Number of workers
     */
    @Override
    public int getWorkerCount() {
        return fragment.fnum();
    }

    /**
     * Get index for this worker
     *
     * @return Index of this worker
     */
    @Override
    public int getMyWorkerIndex() {
        return fragment.fid();
    }

    /**
     * Get worker index which will contain vertex with given id, if such vertex exists.
     *
     * @param vertexId vertex id
     * @return worker index
     */
    @Override
    public int getWorkerForVertex(OID_T vertexId) {
        return 0;
    }

    @Override
    public void initialize(
            GraphState graphState,
            WorkerClientRequestProcessor<OID_T, VDATA_T, EDATA_T> workerClientRequestProcessor,
            CentralizedServiceWorker<OID_T, VDATA_T, EDATA_T> serviceWorker,
            WorkerGlobalCommUsage workerGlobalCommUsage) {}
}
