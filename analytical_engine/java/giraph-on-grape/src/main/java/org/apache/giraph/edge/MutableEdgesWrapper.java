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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Helper class that wraps the current out-edges and inserts them into a new
 * data structure as they are iterated over.
 * Used by Vertex to provide a mutable iterator when the chosen
 * {@link OutEdges} doesn't offer a specialized one.
 * The edges are "unwrapped" back to the chosen {@link OutEdges} data
 * structure as soon as possible: either when the iterator is exhausted,
 * or after compute() if iteration has been terminated early.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class MutableEdgesWrapper<I extends WritableComparable,
    E extends Writable> implements OutEdges<I, E> {
    /** New edges data structure (initially empty). */
    private final OutEdges<I, E> newEdges;
    /** Iterator over the old edges. */
    private final Iterator<Edge<I, E>> oldEdgesIterator;
    /** Last edge that was returned during iteration. */
    private MutableEdge<I, E> currentEdge;
    /** Number of edges. */
    private int numEdges;

    /**
     * Private constructor: instantiation happens through the {@code wrap()}
     * factory method.
     *
     * @param oldEdges Current out-edges
     * @param newEdges New (empty) edges data structure
     */
    private MutableEdgesWrapper(OutEdges<I, E> oldEdges,
        OutEdges<I, E> newEdges) {
        oldEdgesIterator = oldEdges.iterator();
        this.newEdges = newEdges;
        numEdges = oldEdges.size();
    }

    /**
     * Factory method to create a new wrapper over the existing out-edges.
     *
     * @param edges Current out-edges
     * @param conf Configuration
     * @param <I> Vertex id
     * @param <E> Edge value
     * @return The wrapped edges
     */
//    public static <I extends WritableComparable, E extends Writable>
//    MutableEdgesWrapper<I, E> wrap(
//        OutEdges<I, E> edges,
//        ImmutableClassesGiraphConfiguration<I, ?, E> conf) {
//        MutableEdgesWrapper<I, E> wrapper = new MutableEdgesWrapper<I, E>(
//            edges, conf.createAndInitializeOutEdges(edges.size()));
//        return wrapper;
//    }

    /**
     * Moves all the remaining edges to the new data structure, and returns it.
     *
     * @return The new {@link OutEdges} data structure.
     */
    public OutEdges<I, E> unwrap() {
        if (currentEdge != null) {
            newEdges.add(currentEdge);
            currentEdge = null;
        }
        while (oldEdgesIterator.hasNext()) {
            newEdges.add(oldEdgesIterator.next());
        }
        return newEdges;
    }

    /**
     * Get the new {@link OutEdges} data structure.
     *
     * @return New edges
     */
    public OutEdges<I, E> getNewEdges() {
        return newEdges;
    }

    /**
     * Get the iterator over the old edges data structure.
     *
     * @return Old edges iterator
     */
    public Iterator<Edge<I, E>> getOldEdgesIterator() {
        return oldEdgesIterator;
    }

    /**
     * Get the last edge returned by the mutable iterator.
     *
     * @return Last edge iterated on
     */
    public MutableEdge<I, E> getCurrentEdge() {
        return currentEdge;
    }

    /**
     * Set the last edge returned by the mutable iterator.
     *
     * @param edge Last edge iterated on
     */
    public void setCurrentEdge(MutableEdge<I, E> edge) {
        currentEdge = edge;
    }

    /**
     * Decrement the number of edges (to account for a deletion from the mutable
     * iterator).
     */
    public void decrementEdges() {
        --numEdges;
    }

    @Override
    public void initialize(Iterable<Edge<I, E>> edges) {
        throw new IllegalStateException("initialize: MutableEdgesWrapper should " +
            "never be initialized.");
    }

    @Override
    public void initialize(int capacity) {
        throw new IllegalStateException("initialize: MutableEdgesWrapper should " +
            "never be initialized.");
    }

    @Override
    public void initialize() {
        throw new IllegalStateException("initialize: MutableEdgesWrapper should " +
            "never be initialized.");
    }

    @Override
    public void add(Edge<I, E> edge) {
        unwrap().add(edge);
    }

    @Override
    public void remove(I targetVertexId) {
        unwrap().remove(targetVertexId);
    }

    @Override
    public int size() {
        return numEdges;
    }

    @Override
    public Iterator<Edge<I, E>> iterator() {
        return unwrap().iterator();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IllegalStateException("write: MutableEdgesWrapper should " +
            "never be serialized.");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IllegalStateException("readFields: MutableEdgesWrapper should " +
            "never be deserialized.");
    }
}
