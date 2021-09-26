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

package org.apache.giraph.worker;

import com.alibaba.graphscope.fragment.IFragment;

import org.apache.giraph.graph.AggregatorManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * WorkerContext allows for the execution of user code on a per-worker basis. There's one
 * WorkerContext per worker.
 *
 * <p>Giraph worker context is abstract. Our implementation should contains all necessary interfaces
 * needed. see {@link org.apache.giraph.worker.impl.DefaultWorkerContext}
 */
@SuppressWarnings("rawtypes")
public abstract class WorkerContext
        implements WorkerAggregator, Writable, WorkerIndexUsage<WritableComparable> {

    private static Logger logger = LoggerFactory.getLogger(WorkerContext.class);

    private IFragment fragment;
    /** Set to -1, so if not manually set, error will be reported. */
    private int curStep = -1;

    private AggregatorManager aggregatorManager;

    public void setFragment(IFragment fragment) {
        this.fragment = fragment;
    }

    public void setAggregatorManager(AggregatorManager aggregatorManager) {
        this.aggregatorManager = aggregatorManager;
    }

    /**
     * Make sure this function is called after each step.
     *
     * @param step
     */
    public void setCurStep(int step) {
        this.curStep = step;
    }

    public void incStep() {
        this.curStep += 1;
    }

    /**
     * Initialize the WorkerContext. This method is executed once on each Worker before the first
     * superstep starts.
     *
     * @throws IllegalAccessException Thrown for getting the class
     * @throws InstantiationException Expected instantiation in this method.
     */
    public abstract void preApplication() throws InstantiationException, IllegalAccessException;

    /**
     * Finalize the WorkerContext. This method is executed once on each Worker after the last
     * superstep ends.
     */
    public abstract void postApplication();

    /**
     * Execute user code. This method is executed once on each Worker before each superstep starts.
     */
    public abstract void preSuperstep();

    /**
     * Get number of workers.
     *
     * <p>We use fragment fnum to represent fragment number.
     *
     * @return Number of workers
     */
    @Override
    public final int getWorkerCount() {
        if (Objects.isNull(fragment)) {
            logger.error("Fragment null, please set fragment first");
            return 0;
        }
        return fragment.fnum();
    }

    /**
     * Get index for this worker
     *
     * @return Index of this worker
     */
    @Override
    public final int getMyWorkerIndex() {
        if (Objects.isNull(fragment)) {
            logger.error("Fragment null, please set fragment first");
            return 0;
        }
        return fragment.fid();
    }

    @Override
    public final int getWorkerForVertex(WritableComparable vertexId) {
        logger.error("Not implemented");
        return 0;
    }

    /**
     * Get messages which other workers sent to this worker and clear them (can be called once per
     * superstep)
     *
     * @return Messages received
     */
    public List<Writable> getAndClearMessagesFromOtherWorkers() {
        logger.error("Not implemented");
        return null;
    }

    /**
     * Send message to another worker
     *
     * @param message Message to send
     * @param workerIndex Index of the worker to send the message to
     */
    public void sendMessageToWorker(Writable message, int workerIndex) {
        logger.error("Not implemented");
    }

    /** Execute user code. This method is executed once on each Worker after each superstep ends. */
    public abstract void postSuperstep();

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    public long getSuperstep() {
        return curStep;
    }

    /**
     * Get the total (all workers) number of vertices that existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep) (?)
     */
    public final long getTotalNumVertices() {
        if (Objects.isNull(fragment)) {
            logger.error("Fragment null, please set fragment first");
            return 0;
        }
        return fragment.getTotalVerticesNum();
    }

    /**
     * Get the total (all workers) number of edges that existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    public final long getTotalNumEdges() {
        if (Objects.isNull(fragment)) {
            logger.error("Fragment null, please set fragment first");
            return 0;
        }
        return fragment.getEdgeNum();
    }

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    public final Mapper.Context getContext() {
        logger.error("No mapper context available");
        return null;
    }

    /**
     * Call this to log a line to command line of the job. Use in moderation - it's a synchronous
     * call to Job client
     *
     * @param line Line to print
     */
    public void logToCommandLine(String line) {
        logger.info(line);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {}

    @Override
    public void readFields(DataInput dataInput) throws IOException {}

    /** Methods provided by CommunicatorImpl. */

    /**
     * Reduce value by name.
     *
     * @param name key
     * @param value value
     */
    @Override
    public void reduce(String name, Object value) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Set communicator first");
            return;
        }
    }

    @Override
    public void reduceMerge(String name, Writable value) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Set communicator first");
            return;
        }
    }

    @Override
    public <B extends Writable> B getBroadcast(String name) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Set communicator first");
            return null;
        }
        // TODO: fix
        return null;
    }

    @Override
    public <A extends Writable> void aggregate(String name, A value) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Set communicator first");
            return;
        }
        aggregatorManager.aggregate(name, value);
    }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        if (Objects.isNull(aggregatorManager)) {
            logger.error("Set communicator first");
            return null;
        }
        return aggregatorManager.getAggregatedValue(name);
    }
}
