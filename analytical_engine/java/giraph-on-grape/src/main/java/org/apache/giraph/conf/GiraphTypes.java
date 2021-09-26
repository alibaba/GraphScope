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

import static org.apache.giraph.conf.GiraphConstants.EDGE_VALUE_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_ID_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_VALUE_CLASS;
import static org.apache.giraph.conf.GiraphConstants.OUTGOING_MESSAGE_VALUE_CLASS;
import static org.apache.giraph.conf.GiraphConstants.INCOMING_MESSAGE_VALUE_CLASS;
import static org.apache.giraph.utils.ConfigurationUtils.getTypesHolderClass;
import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Holder for the generic types that describe user's graph.
 *
 * @param <I> Vertex ID class
 * @param <V> Vertex Value class
 * @param <E> Edge class
 */
public class GiraphTypes<I extends WritableComparable, V extends Writable,
    E extends Writable> {
    /** Vertex id class */
    private Class<I> vertexIdClass;
    /** Vertex value class */
    private Class<V> vertexValueClass;
    /** Edge value class */
    private Class<E> edgeValueClass;
    /** Outgoing message value class */
    private Class<? extends Writable> outgoingMessageValueClass;
    /** Incoming message value class */
    private Class<? extends Writable> incomingMessageValueClass;
    /** Vertex implementation class */
    private Class<? extends Vertex> vertexClass = VertexImpl.class;


    /**
     * Empty Constructor
     */
    public GiraphTypes() { }

    /**
     * Constructor taking values
     *
     * @param vertexIdClass vertex id class
     * @param vertexValueClass vertex value class
     * @param edgeValueClass edge value class
     * @param incomingMessageValueClass incoming message class
     * @param outgoingMessageValueClass outgoing message class
     */
    public GiraphTypes(Class<I> vertexIdClass,
        Class<V> vertexValueClass,
        Class<E> edgeValueClass,
        Class<? extends Writable> incomingMessageValueClass,
        Class<? extends Writable> outgoingMessageValueClass) {
        this.edgeValueClass = edgeValueClass;
        this.outgoingMessageValueClass = outgoingMessageValueClass;
        this.incomingMessageValueClass = incomingMessageValueClass;
        this.vertexIdClass = vertexIdClass;
        this.vertexValueClass = vertexValueClass;
    }

    /**
     * Read types from a {@link Configuration}.
     * First tries to read them directly from the configuration options.
     * If that doesn't work, tries to infer from.
     *
     * @param conf Configuration
     * @param <IX> vertex id
     * @param <VX> vertex value
     * @param <EX> edge value
     * @return GiraphTypes
     */
    public static <IX extends WritableComparable, VX extends Writable,
        EX extends Writable> GiraphTypes<IX, VX, EX> readFrom(
        Configuration conf) {
        GiraphTypes<IX, VX, EX> types = new GiraphTypes<IX, VX, EX>();
        types.readDirect(conf);
        if (!types.hasData()) {
            Class<? extends TypesHolder> klass = getTypesHolderClass(conf);
            if (klass != null) {
                types.inferFrom(klass);
            }
        }
        return types;
    }


    /**
     * Read types directly from Configuration
     *
     * @param conf Configuration
     */
    private void readDirect(Configuration conf) {
        vertexIdClass = (Class<I>) VERTEX_ID_CLASS.get(conf);
        vertexValueClass = (Class<V>) VERTEX_VALUE_CLASS.get(conf);
        edgeValueClass = (Class<E>) EDGE_VALUE_CLASS.get(conf);
        outgoingMessageValueClass = OUTGOING_MESSAGE_VALUE_CLASS.get(conf);
        incomingMessageValueClass = INCOMING_MESSAGE_VALUE_CLASS.get(conf);
        vertexClass = VERTEX_CLASS.get(conf);
    }

    /**
     * Infer types from Computation class
     *
     * @param klass Computation class
     */
    public void inferFrom(Class<? extends TypesHolder> klass) {
        Class<?>[] classList = getTypeArguments(TypesHolder.class, klass);
        Preconditions.checkArgument(classList.length == 5);
        vertexIdClass = (Class<I>) classList[0];
        vertexValueClass = (Class<V>) classList[1];
        edgeValueClass = (Class<E>) classList[2];
        incomingMessageValueClass = (Class<? extends Writable>) classList[3];
        outgoingMessageValueClass = (Class<? extends Writable>) classList[4];
    }

    /**
     * Check if types are set
     *
     * @return true if types are set
     */
    public boolean hasData() {
        return vertexIdClass != null &&
            vertexValueClass != null &&
            edgeValueClass != null &&
            incomingMessageValueClass != null &&
            outgoingMessageValueClass != null;
    }

    /**
     * Write types to Configuration
     *
     * @param conf Configuration
     */
    public void writeTo(Configuration conf) {
        VERTEX_ID_CLASS.set(conf, vertexIdClass);
        VERTEX_VALUE_CLASS.set(conf, vertexValueClass);
        EDGE_VALUE_CLASS.set(conf, edgeValueClass);
        INCOMING_MESSAGE_VALUE_CLASS.set(conf, incomingMessageValueClass);
        OUTGOING_MESSAGE_VALUE_CLASS.set(conf, outgoingMessageValueClass);
    }

    /**
     * Write types to Configuration if not already set
     *
     * @param conf Configuration
     */
    public void writeIfUnset(Configuration conf) {
        VERTEX_ID_CLASS.setIfUnset(conf, vertexIdClass);
        VERTEX_VALUE_CLASS.setIfUnset(conf, vertexValueClass);
        EDGE_VALUE_CLASS.setIfUnset(conf, edgeValueClass);
        INCOMING_MESSAGE_VALUE_CLASS.setIfUnset(conf, incomingMessageValueClass);
        OUTGOING_MESSAGE_VALUE_CLASS.setIfUnset(conf, outgoingMessageValueClass);
    }

    public Class<E> getEdgeValueClass() {
        return edgeValueClass;
    }

    Class<? extends Writable> getInitialOutgoingMessageValueClass() {
        return outgoingMessageValueClass;
    }
    Class<? extends Writable> getInitialIncomingMessageValueClass() {
        return incomingMessageValueClass;
    }

    public Class<I> getVertexIdClass() {
        return vertexIdClass;
    }

    public Class<V> getVertexValueClass() {
        return vertexValueClass;
    }

    public Class<? extends Vertex> getVertexClass() {
        return vertexClass;
    }

    public void setEdgeValueClass(Class<E> edgeValueClass) {
        this.edgeValueClass = edgeValueClass;
    }

    public void setVertexIdClass(Class<I> vertexIdClass) {
        this.vertexIdClass = vertexIdClass;
    }

    public void setVertexValueClass(Class<V> vertexValueClass) {
        this.vertexValueClass = vertexValueClass;
    }

    public void setOutgoingMessageValueClass(
        Class<? extends Writable> outgoingMessageValueClass) {
        this.outgoingMessageValueClass = outgoingMessageValueClass;
    }
    public void setIncomingMessageValueClass(
        Class<? extends Writable> incomingMessageValueClass) {
        this.outgoingMessageValueClass = incomingMessageValueClass;
    }

    public Class<? extends Writable> getIncomingMessageValueClass(){
        return this.incomingMessageValueClass;
    }

    public Class<? extends Writable> getOutgoingMessageValueClass(){
        return this.outgoingMessageValueClass;
    }
}
