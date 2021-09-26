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

package org.apache.giraph.conf;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;

/**
 * Adds user methods specific to Giraph. This will be put into an ImmutableClassesGiraphConfiguration
 * that provides the configuration plus the immutable classes.
 *
 * <p>Keeps track of parameters which were set so it easily set them in another copy of
 * configuration.
 */
public class GiraphConfiguration extends Configuration implements GiraphConstants {

    /**
     * ByteBufAllocator to be used by netty
     */
    private ByteBufAllocator nettyBufferAllocator = null;

    /**
     * Constructor that creates the configuration
     */
    public GiraphConfiguration() {
    }

    /**
     * Constructor.
     *
     * @param conf Configuration
     */
    public GiraphConfiguration(Configuration conf) {
        super(conf);
    }


    /**
     * Set the worker context class (optional)
     *
     * @param workerContextClass Determines what code is executed on a each worker before and after
     *                           each superstep and computation
     */
    public final void setWorkerContextClass(Class<? extends WorkerContext> workerContextClass) {
        WORKER_CONTEXT_CLASS.set(this, workerContextClass);
    }

    public Class<? extends WorkerContext> getWorkerContextClass() {
        return WORKER_CONTEXT_CLASS.get(this);
    }

    /**
     * Set the computation class(user app).
     *
     * @param appClass User specified computation class.
     */
    public final void setComputationClass(Class<? extends AbstractComputation> appClass) {
        COMPUTATION_CLASS.set(this, appClass);
    }

    /**
     * Get the user's subclassed {@link Computation}
     *
     * @return User's computation class
     */
    public Class<? extends Computation> getComputationClass() {
        return COMPUTATION_CLASS.get(this);
    }

    /**
     * Set vertex input class.
     *
     * @param vertexInputFormatClass User specified computation class.
     */
    public final void setVertexInputFormatClass(
        Class<? extends VertexInputFormat> vertexInputFormatClass) {
        VERTEX_INPUT_FORMAT_CLASS.set(this, vertexInputFormatClass);
    }

    /**
     * get vertex input class.
     *
     */
    public final Class<? extends VertexInputFormat> getVertexInputFormatClass() {
        return VERTEX_INPUT_FORMAT_CLASS.get(this);
    }

    /**
     * Set vertex input class.
     *
     * @param edgeInputFormatClass User specified computation class.
     */
    public final void setEdgeInputFormatClass(
        Class<? extends EdgeInputFormat> edgeInputFormatClass) {
        EDGE_INPUT_FORMAT_CLASS.set(this, edgeInputFormatClass);
    }

//    /**
//     * get vertex input class.
//     *
//     */
//    public final Class<? extends EdgeInputFormat> getEdgeInputFormatClass() {
//        return EDGE_INPUT_FORMAT_CLASS.get(this);
//    }

    /**
     * Does the job have a {@link org.apache.giraph.io.VertexOutputFormat}?
     *
     * @return True iff a {@link org.apache.giraph.io.VertexOutputFormat} has been specified.
     */
    public boolean hasVertexOutputFormat() {
        return VERTEX_OUTPUT_FORMAT_CLASS.get(this) != null;
    }

//    /**
//     * Set vertex output format class.
//     *
//     * @param vertexOutputFormatClass User specified computation class.
//     */
//    public final void setVertexOutputFormatClass(Class<? extends VertexOutputFormat> vertexOutputFormatClass){
//        VERTEX_OUTPUT_FORMAT_CLASS.set(this, vertexOutputFormatClass);
//    }

//    /**
//     * Set vertex input class.
//     *
//     */
//    public final Class<? extends VertexOutputFormat> getVertexOutputFormatClass(){
//        return VERTEX_OUTPUT_FORMAT_CLASS.get(this);
//    }
     public final void setVertexOutputFormatClass(
            Class<? extends VertexOutputFormat> vertexOutputFormatClass) {
        VERTEX_OUTPUT_FORMAT_CLASS.set(this, vertexOutputFormatClass);
     }


    /**
     * Set the message combiner class (optional)
     *
     * @param messageCombinerClass Determines how vertex messages are combined
     */
    public void setMessageCombinerClass(Class<? extends MessageCombiner> messageCombinerClass) {
        MESSAGE_COMBINER_CLASS.set(this, messageCombinerClass);
    }

    /**
     * Set the master class (optional)
     *
     * @param masterComputeClass Runs master computation
     */
    public final void setMasterComputeClass(Class<? extends MasterCompute> masterComputeClass) {
        MASTER_COMPUTE_CLASS.set(this, masterComputeClass);
    }

    /**
     * Does the job have a {@link VertexOutputFormat} subdir?
     *
     * @return True iff a {@link VertexOutputFormat} subdir has been specified.
     */
    public boolean hasVertexOutputFormatSubdir() {
        return !VERTEX_OUTPUT_FORMAT_SUBDIR.get(this).isEmpty();
    }

    /**
     * Set the vertex output format path
     *
     * @param path path where the vertices will be written
     */
    public final void setVertexOutputFormatSubdir(String path) {
        VERTEX_OUTPUT_FORMAT_SUBDIR.set(this, path);
    }

    /**
     * The the path where we output.
     *
     * @param path output path.
     */
    public final void setVertexOutputPath(String path) {
        VERTEX_OUTPUT_PATH.set(this, path);
    }

    /**
     * Used by netty client and server to create ByteBufAllocator
     *
     * @return ByteBufAllocator
     */
    public ByteBufAllocator getNettyAllocator() {
        if (nettyBufferAllocator == null) {
            if (NETTY_USE_POOLED_ALLOCATOR.get(this)) { // Use pooled allocator
                nettyBufferAllocator =
                    new PooledByteBufAllocator(NETTY_USE_DIRECT_MEMORY.get(this));
            } else { // Use un-pooled allocator
                // Note: Current default settings create un-pooled heap allocator
                nettyBufferAllocator =
                    new UnpooledByteBufAllocator(NETTY_USE_DIRECT_MEMORY.get(this));
            }
        }
        return nettyBufferAllocator;
    }


    public int getNettyServerExecutionThreads() {
        return NETTY_SERVER_EXECUTION_THREADS.get(this);
    }

    /**
     * Return local host name by default. Or local host IP if preferIP option is set.
     *
     * @return local host name or IP
     */
    public String getLocalHostOrIp() throws UnknownHostException {
            return InetAddress.getLocalHost().getHostAddress();
    }

    /**
     * Get the init port for message manager(Netty base).
     * @return port
     */
    public int getMessagerInitServerPort(){
        return MESSAGE_MANAGER_BASE_SERVER_PORT.get(this);
    }

    /**
     * Get the initial port for netty server used in aggregator
     * @return port
     */
    public int getAggregatorServerInitPort(){
        return AGGREGATOR_BASE_SERVER_PORT.get(this);
    }


    public void setEdgeManager(String edgeManager){
        EDGE_MANAGER.set(this, edgeManager);
    }

    public String getEdgeManager(){
        return EDGE_MANAGER.get(this);
    }
}
