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
 * A complete edge, the target vertex and the edge value.  Can only be one
 * edge with a destination vertex id per edge map. This edge can be reused,
 * that is you can set it's target vertex ID and edge value.
 * Note: this class is useful for certain optimizations,
 * but it's not meant to be exposed to the user. Look at {@link MutableEdge}
 * instead.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
public interface ReusableEdge<I extends WritableComparable, E extends Writable>
    extends MutableEdge<I, E> {
    /**
     * Set the destination vertex index of this edge.
     *
     * @param targetVertexId new destination vertex
     */
    void setTargetVertexId(I targetVertexId);
}
