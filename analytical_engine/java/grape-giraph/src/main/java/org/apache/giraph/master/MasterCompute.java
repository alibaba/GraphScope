/*
 * The file GiraphConstants.java is referred and derived from
 * project apache/Giraph,
 *
 *    https://github.com/apache/giraph
 * giraph-core/src/main/java/org/apache/giraph/master/MasterCompute.java
 *
 * which has the following license:
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
package org.apache.giraph.master;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.AggregatorManager;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for defining a master vertex that can perform centralized computation between
 * supersteps. This class will be instantiated on the master node and will run every superstep
 * before the workers do.
 *
 * <p>Communication with the workers should be performed via aggregators. The values of the
 * aggregators are broadcast to the workers before vertex.compute() is called and collected by the
 * master before master.compute() is called. This means aggregator values used by the workers are
 * consistent with aggregator values from the master from the same superstep and aggregator used by
 * the master are consistent with aggregator values from the workers from the previous superstep.
 */
public abstract class MasterCompute extends DefaultImmutableClassesGiraphConfigurable
        implements MasterAggregatorUsage, MasterGlobalCommUsage, Writable {

    private static Logger logger = LoggerFactory.getLogger(MasterCompute.class);
    /**
     * If true, do not do anymore computation on this vertex.
     */
    private boolean halt = false;
    /**
     * Fragment
     */
    private IFragment fragment;
    /**
     * super step
     */
    private int superStep;
    /**
     * Master aggregator usage
     */
    private AggregatorManager aggregatorManager;
    /** Graph state */
    //    private GraphState graphState;
    /**
     * Computation and MessageCombiner classes used, which can be switched by master
     */
    private SuperstepClasses superstepClasses;

    public void setFragment(IFragment fragment) {
        this.fragment = fragment;
    }

    public void setSuperStep(int superStep) {
        this.superStep = superStep;
    }

    public void incSuperStep() {
        this.superStep += 1;
    }

    public void setAggregatorManager(AggregatorManager aggregatorManager) {
        this.aggregatorManager = aggregatorManager;
    }

    /**
     * Must be defined by user to specify what the master has to do.
     */
    public abstract void compute();

    /**
     * Initialize the MasterCompute class, this is the place to register aggregators.
     *
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public abstract void initialize() throws InstantiationException, IllegalAccessException;

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    public final long getSuperstep() {
        return superStep;
    }

    /**
     * Get the total (all workers) number of vertices that existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    public final long getTotalNumVertices() {
        return fragment.getTotalVerticesNum();
    }

    /**
     * Get the total (all workers) number of edges that existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    public final long getTotalNumEdges() {
        return fragment.getEdgeNum();
    }

    /**
     * After this is called, the computation will stop, even if there are still messages in the
     * system or vertices that have not voted to halt.
     */
    public final void haltComputation() {
        halt = true;
    }

    /**
     * Has the master halted?
     *
     * @return True if halted, false otherwise.
     */
    public final boolean isHalted() {
        return halt;
    }

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    public final Mapper.Context getContext() {
        logger.error("No mapper context in grape-giraph");
        return null;
    }

    /**
     * Get Computation class to be used
     *
     * @return Computation class
     */
    public final Class<? extends Computation> getComputation() {
        // Might be called prior to classes being set, do not return NPE
        if (superstepClasses == null) {
            return null;
        }

        return superstepClasses.getComputationClass();
    }

    /**
     * Set Computation class to be used
     *
     * @param computationClass Computation class
     */
    public final void setComputation(Class<? extends Computation> computationClass) {
        superstepClasses.setComputationClass(computationClass);
    }

    /**
     * Get MessageCombiner class to be used
     *
     * @return MessageCombiner class
     */
    public final Class<? extends MessageCombiner> getMessageCombiner() {
        // Might be called prior to classes being set, do not return NPE
        if (superstepClasses == null) {
            return null;
        }

        return superstepClasses.getMessageCombinerClass();
    }

    /**
     * Set MessageCombiner class to be used
     *
     * @param combinerClass MessageCombiner class
     */
    public final void setMessageCombiner(Class<? extends MessageCombiner> combinerClass) {
        superstepClasses.setMessageCombinerClass(combinerClass);
    }

    /**
     * Set incoming message class to be used
     *
     * @param incomingMessageClass incoming message class
     */
    @Deprecated
    public final void setIncomingMessage(Class<? extends Writable> incomingMessageClass) {
        superstepClasses.setIncomingMessageClass(incomingMessageClass);
    }

    /**
     * Set outgoing message class to be used
     *
     * @param outgoingMessageClass outgoing message class
     */
    public final void setOutgoingMessage(Class<? extends Writable> outgoingMessageClass) {
        superstepClasses.setOutgoingMessageClass(outgoingMessageClass);
    }

    /**
     * Set outgoing message classes to be used
     *
     * @param outgoingMessageClasses outgoing message classes
     */
    public void setOutgoingMessageClasses(
            MessageClasses<? extends WritableComparable, ? extends Writable>
                    outgoingMessageClasses) {
        superstepClasses.setOutgoingMessageClasses(outgoingMessageClasses);
    }

    @Override
    public final <S, R extends Writable> void registerReducer(
            String name, ReduceOperation<S, R> reduceOp) {
        //        serviceMaster.getGlobalCommHandler().registerReducer(name, reduceOp);
    }

    @Override
    public final <S, R extends Writable> void registerReducer(
            String name, ReduceOperation<S, R> reduceOp, R globalInitialValue) {
        //        serviceMaster.getGlobalCommHandler().registerReducer(
        //            name, reduceOp, globalInitialValue);
    }

    @Override
    public final <T extends Writable> T getReduced(String name) {
        //        return serviceMaster.getGlobalCommHandler().getReduced(name);
        return null;
    }

    @Override
    public final void broadcast(String name, Writable object) {
        //        serviceMaster.getGlobalCommHandler().broadcast(name, object);
    }

    @Override
    public final <A extends Writable> boolean registerAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return aggregatorManager.registerAggregator(name, aggregatorClass);
    }

    @Override
    public final <A extends Writable> boolean registerPersistentAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return aggregatorManager.registerPersistentAggregator(name, aggregatorClass);
    }

    @Override
    public final <A extends Writable> A getAggregatedValue(String name) {
        return aggregatorManager.getAggregatedValue(name);
    }

    @Override
    public final <A extends Writable> void setAggregatedValue(String name, A value) {
        aggregatorManager.setAggregatedValue(name, value);
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
}
