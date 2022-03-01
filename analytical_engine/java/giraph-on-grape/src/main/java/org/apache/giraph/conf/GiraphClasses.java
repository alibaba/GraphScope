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

import com.alibaba.graphscope.parallel.message.MessageEncodeAndStoreType;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holder for classes used by Giraph.
 *
 * @param <I> Vertex ID class
 * @param <V> Vertex Value class
 * @param <E> Edge class
 */
@SuppressWarnings("unchecked")
public class GiraphClasses<I extends WritableComparable,
    V extends Writable, E extends Writable> implements GiraphConstants {

    private static Logger logger = LoggerFactory.getLogger(GiraphClasses.class);


    /**
     * Generic types used to describe graph
     */
    protected GiraphTypes<I, V, E> giraphTypes;

    /**
     * Vertex input format class - cached for fast access
     */
    protected Class<? extends VertexInputFormat<I, V, E>>
        vertexInputFormatClass;

    protected Class<? extends EdgeInputFormat<I,E>> edgeInputFormatClass;
    /**
     * Vertex output format class - cached for fast access
     */
    protected Class<? extends VertexOutputFormat<I, V, E>>
        vertexOutputFormatClass;

    /**
     * Computation class - cached for fast access
     */
    protected Class<? extends Computation<I, V, E,
        ? extends Writable, ? extends Writable>>
        computationClass;
    /**
     * Worker context class - cached for fast access
     */
    protected Class<? extends WorkerContext> workerContextClass;
    /** Master compute class - cached for fast access */
    protected Class<? extends MasterCompute> masterComputeClass;

//    /** Edge input format class - cached for fast access */
//    protected Class<? extends EdgeInputFormat<I, E>>
//        edgeInputFormatClass;
//    /** Edge output format class - cached for fast access */
//    protected Class<? extends EdgeOutputFormat<I, V, E>>
//        edgeOutputFormatClass;
    /** Incoming message classes */
    protected MessageClasses<I, ? extends Writable> incomingMessageClasses;
    /** Outgoing message classes */
    protected MessageClasses<I, ? extends Writable> outgoingMessageClasses;

    public GiraphClasses() {
        giraphTypes = new GiraphTypes<I, V, E>();
        masterComputeClass = DefaultMasterCompute.class;
        workerContextClass = DefaultWorkerContext.class;
    }

    /**
     * Constructor that reads classes from a Configuration object.
     *
     * @param conf Configuration object to read from.
     */
    public GiraphClasses(Configuration conf) {
        giraphTypes = GiraphTypes.readFrom(conf);
//        computationFactoryClass =
//            (Class<? extends ComputationFactory<I, V, E,
//                ? extends Writable, ? extends Writable>>)
//                COMPUTATION_FACTORY_CLASS.get(conf);
        // logger.info(
        //     "vertexId class: " + giraphTypes.getVertexIdClass().getName() + ", vertex value class: "
        //         + giraphTypes.getVertexValueClass().getName() + ", edge value class: " + giraphTypes
        //         .getEdgeValueClass().getName());
        computationClass =
            (Class<? extends Computation<I, V, E,
                ? extends Writable, ? extends Writable>>)
                COMPUTATION_CLASS.get(conf);
        // logger.info("Setting computation class to: " + computationClass.getSimpleName());

//        outEdgesClass = (Class<? extends OutEdges<I, E>>)
//            VERTEX_EDGES_CLASS.get(conf);
//        inputOutEdgesClass = (Class<? extends OutEdges<I, E>>)
//            INPUT_VERTEX_EDGES_CLASS.getWithDefault(conf, outEdgesClass);

//        graphPartitionerFactoryClass =
//            (Class<? extends GraphPartitionerFactory<I, V, E>>)
//                GRAPH_PARTITIONER_FACTORY_CLASS.get(conf);

        vertexInputFormatClass = (Class<? extends VertexInputFormat<I, V, E>>)
            VERTEX_INPUT_FORMAT_CLASS.get(conf);
        vertexOutputFormatClass = (Class<? extends VertexOutputFormat<I, V, E>>)
            VERTEX_OUTPUT_FORMAT_CLASS.get(conf);
        edgeInputFormatClass = (Class<? extends EdgeInputFormat<I, E>>)
            EDGE_INPUT_FORMAT_CLASS.get(conf);
//        edgeOutputFormatClass = (Class<? extends EdgeOutputFormat<I, V, E>>)
//            EDGE_OUTPUT_FORMAT_CLASS.get(conf);
//        mappingInputFormatClass = (Class<? extends MappingInputFormat<I, V, E,
//            ? extends Writable>>)
//            MAPPING_INPUT_FORMAT_CLASS.get(conf);

//        aggregatorWriterClass = AGGREGATOR_WRITER_CLASS.get(conf);

        // incoming messages shouldn't be used in first iteration at all
        // but empty message stores are created, etc, so using NoMessage
        // to enforce not a single message is read/written
//        incomingMessageClasses = new DefaultMessageClasses(
//            NoMessage.class,
//            DefaultMessageValueFactory.class,
//            null,
//            MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION);
//        outgoingMessageClasses = new DefaultMessageClasses(
//            giraphTypes.getInitialOutgoingMessageValueClass(),
//            OUTGOING_MESSAGE_VALUE_FACTORY_CLASS.get(conf),
//            MESSAGE_COMBINER_CLASS.get(conf),
//            MESSAGE_ENCODE_AND_STORE_TYPE.get(conf));

//        vertexResolverClass = (Class<? extends VertexResolver<I, V, E>>)
//            VERTEX_RESOLVER_CLASS.get(conf);
//        vertexValueCombinerClass = (Class<? extends VertexValueCombiner<V>>)
//            VERTEX_VALUE_COMBINER_CLASS.get(conf);
        workerContextClass = WORKER_CONTEXT_CLASS.get(conf);
        masterComputeClass =  MASTER_COMPUTE_CLASS.get(conf);
//        partitionClass = (Class<? extends Partition<I, V, E>>)
//            PARTITION_CLASS.get(conf);
//
//        edgeInputFilterClass = (Class<? extends EdgeInputFilter<I, E>>)
//            EDGE_INPUT_FILTER_CLASS.get(conf);
//        vertexInputFilterClass = (Class<? extends VertexInputFilter<I, V, E>>)
//            VERTEX_INPUT_FILTER_CLASS.get(conf);



        incomingMessageClasses = new DefaultMessageClasses(
            giraphTypes.getInitialIncomingMessageValueClass(),
            DefaultMessageValueFactory.class,
            MESSAGE_COMBINER_CLASS.get(conf), //In giraph, here is set to null.
	    MessageEncodeAndStoreType.SIMPLE_MESSAGE_STORE);
        outgoingMessageClasses = new DefaultMessageClasses(
            giraphTypes.getInitialOutgoingMessageValueClass(),
            OUTGOING_MESSAGE_VALUE_FACTORY_CLASS.get(conf),
            MESSAGE_COMBINER_CLASS.get(conf),
            MessageEncodeAndStoreType.SIMPLE_MESSAGE_STORE);
    }

    /**
     * Get Vertex implementation class
     *
     * @return Vertex implementation class
     */
    public Class<? extends Vertex> getVertexClass() {
        return giraphTypes.getVertexClass();
    }

    /**
     * Check if VertexOutputFormat is set
     *
     * @return true if VertexOutputFormat is set
     */
    public boolean hasVertexOutputFormat() {
        return vertexOutputFormatClass != null;
    }

    /**
     * Get VertexOutputFormat set
     *
     * @return VertexOutputFormat
     */
    public Class<? extends VertexOutputFormat<I, V, E>>
    getVertexOutputFormatClass() {
        return vertexOutputFormatClass;
    }

    /**
     * Set WorkerContext used
     *
     * @param workerContextClass WorkerContext class to set
     * @return this
     */
    public GiraphClasses setWorkerContextClass(
        Class<? extends WorkerContext> workerContextClass) {
        this.workerContextClass = workerContextClass;
        return this;
    }

    /**
     * Check if WorkerContext is set
     *
     * @return true if WorkerContext is set
     */
    public boolean hasWorkerContextClass() {
        return workerContextClass != null;
    }

    /**
     * Get WorkerContext used
     *
     * @return WorkerContext
     */
    public Class<? extends WorkerContext> getWorkerContextClass() {
        return workerContextClass;
    }

    /**
     * Get Computation class
     *
     * @return Computation class.
     */
    public Class<? extends Computation<I, V, E,
        ? extends Writable, ? extends Writable>>
    getComputationClass() {
        return computationClass;
    }

    public GiraphTypes<I, V, E> getGiraphTypes() {
        return giraphTypes;
    }

    /**
     * Get Vertex ID class
     *
     * @return Vertex ID class
     */
    public Class<I> getVertexIdClass() {
        return giraphTypes.getVertexIdClass();
    }


    /**
     * Get Vertex Value class
     *
     * @return Vertex Value class
     */
    public Class<V> getVertexValueClass() {
        return giraphTypes.getVertexValueClass();
    }

    /**
     * Get Edge Value class
     *
     * @return Edge Value class
     */
    public Class<E> getEdgeValueClass() {
        return giraphTypes.getEdgeValueClass();
    }


    public MessageClasses<? extends WritableComparable, ? extends Writable>
    getIncomingMessageClasses() {
        return incomingMessageClasses;
    }

    public MessageClasses<? extends WritableComparable, ? extends Writable>
    getOutgoingMessageClasses() {
        return outgoingMessageClasses;
    }

    public Class<? extends Writable> getIncomingMessageClass(){
        return giraphTypes.getIncomingMessageValueClass();
    }

    public Class<? extends  Writable> getOutgoingMessageClass(){
        return giraphTypes.getOutgoingMessageValueClass();
    }

    /**
     * Set Computation class held, and update message types
     *
     * @param computationClass Computation class to set
     * @return this
     */
    public GiraphClasses setComputationClass(Class<? extends
        Computation<I, V, E, ? extends Writable, ? extends Writable>>
        computationClass) {
        this.computationClass = computationClass;
        return this;
    }

    /**
     * Set incoming Message Value class held - messages which have been sent in
     * the previous superstep and are processed in the current one
     *
     * @param incomingMessageClasses Message classes value to set
     * @return this
     */
    public GiraphClasses setIncomingMessageClasses(
        MessageClasses<I, ? extends Writable> incomingMessageClasses) {
        this.incomingMessageClasses = incomingMessageClasses;
        return this;
    }

    /**
     * Set outgoing Message Value class held - messages which are going to be sent
     * during current superstep
     *
     * @param outgoingMessageClasses Message classes value to set
     * @return this
     */
    public GiraphClasses setOutgoingMessageClasses(
        MessageClasses<I, ? extends Writable> outgoingMessageClasses) {
        this.outgoingMessageClasses = outgoingMessageClasses;
        return this;
    }

    /**
     * Check if MasterCompute is set
     *
     * @return true MasterCompute is set
     */
    public boolean hasMasterComputeClass() {
        return masterComputeClass != null;
    }

    /**
     * Get MasterCompute used
     *
     * @return MasterCompute
     */
    public Class<? extends MasterCompute> getMasterComputeClass() {
        return masterComputeClass;
    }

    /**
     * Check if EdgeInputFormat is set
     *
     * @return true if EdgeInputFormat is set
     */
    public boolean hasEdgeInputFormat() {
        return edgeInputFormatClass != null;
    }

    /**
     * Get EdgeInputFormat used
     *
     * @return EdgeInputFormat
     */
    public Class<? extends EdgeInputFormat<I, E>> getEdgeInputFormatClass() {
        return edgeInputFormatClass;
    }

}
