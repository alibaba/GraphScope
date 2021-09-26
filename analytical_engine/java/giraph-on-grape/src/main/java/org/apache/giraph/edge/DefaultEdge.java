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

package org.apache.giraph.edge;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A complete edge, the target vertex and the edge value.  Can only be one edge with a destination
 * vertex id per edge map.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class DefaultEdge<I extends WritableComparable, E extends Writable>
        implements ReusableEdge<I, E> {

    /**
     * Target vertex id
     */
    private I targetVertexId = null;
    /**
     * Edge value
     */
    private E value = null;

    /**
     * Constructor for reflection
     */
    public DefaultEdge() {}

    /**
     * Create the edge with final values. Don't call, use EdgeFactory instead.
     *
     * @param targetVertexId Desination vertex id.
     * @param value          Value of the edge.
     */
    protected DefaultEdge(I targetVertexId, E value) {
        this.targetVertexId = targetVertexId;
        this.value = value;
    }

    @Override
    public I getTargetVertexId() {
        return targetVertexId;
    }

    @Override
    public void setTargetVertexId(I targetVertexId) {
        this.targetVertexId = targetVertexId;
    }

    @Override
    public E getValue() {
        return value;
    }

    @Override
    public void setValue(E value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "(targetVertexId = " + targetVertexId + ", " + "value = " + value + ")";
    }
}
