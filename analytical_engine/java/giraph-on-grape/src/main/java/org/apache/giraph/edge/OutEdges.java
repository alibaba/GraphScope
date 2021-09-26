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
 * Interface for data structures that store out-edges for a vertex.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public interface OutEdges<I extends WritableComparable, E extends Writable>
    extends Iterable<Edge<I, E>>, Writable {
    /**
     * Initialize the data structure and set the edges from an iterable.
     * This method (or one of the two alternatives) must be called
     * after instantiation, unless readFields() is called.
     * Note: whether parallel edges are allowed or not depends on the
     * implementation.
     *
     * @param edges Iterable of edges
     */
    void initialize(Iterable<Edge<I, E>> edges);

    /**
     * Initialize the data structure with the specified initial capacity.
     * This method (or one of the two alternatives) must be called
     * after instantiation, unless readFields() is called.
     *
     * @param capacity Initial capacity
     */
    void initialize(int capacity);

    /**
     * Initialize the data structure with the default initial capacity.
     * This method (or one of the two alternatives) must be called
     * after instantiation, unless readFields() is called.
     *
     */
    void initialize();

    /**
     * Add an edge.
     * Note: whether parallel edges are allowed or not depends on the
     * implementation.
     *
     * @param edge Edge to add
     */
    void add(Edge<I, E> edge);

    /**
     * Remove all edges to the given target vertex.
     * Note: the implementation will vary depending on whether parallel edges
     * are allowed or not.
     *
     * @param targetVertexId Target vertex id
     */
    void remove(I targetVertexId);

    /**
     * Return the number of edges.
     *
     * @return Number of edges
     */
    int size();
}
