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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An edge that has no value.
 *
 * @param <I> Vertex ID
 */
public class EdgeNoValue<I extends WritableComparable> implements ReusableEdge<I, NullWritable> {

    /**
     * Target vertex id
     */
    private I targetVertexId = null;

    /**
     * Empty constructor
     */
    EdgeNoValue() {}

    /**
     * Constructor with target vertex ID. Don't call, use EdgeFactory instead.
     *
     * @param targetVertexId vertex ID
     */
    EdgeNoValue(I targetVertexId) {
        this.targetVertexId = targetVertexId;
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
    public NullWritable getValue() {
        return NullWritable.get();
    }

    @Override
    public void setValue(NullWritable value) {
        // do nothing
    }

    @Override
    public String toString() {
        return "(targetVertexId = " + targetVertexId + ")";
    }
}
