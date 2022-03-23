/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.graph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.MutableEdgesWrapper;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An abstraction for edge management. Including edge id, value get and edge mutation.
 *
 * @param <OID_T>   vertex id type
 * @param <EDATA_T> edge data type.
 */
public interface EdgeManager<OID_T extends WritableComparable, EDATA_T extends Writable> {

    /**
     * Get the number of outgoing edges on a vertex with its lid.
     *
     * @return the total number of outbound edges from this vertex
     */
    int getNumEdges(long lid);

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEdge()) during iteration leads to undefined behavior.
     *
     * @return the out edges (sort order determined by subclass implementation).
     */
    Iterable<Edge<OID_T, EDATA_T>> getEdges(long lid);

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    void setEdges(Iterable<Edge<OID_T, EDATA_T>> edges);

    /**
     * Get an iterable of out-edges that can be modified in-place. This can mean changing the
     * current edge value or removing the current edge (by using the iterator version). Note:
     * accessing the edges with other methods (e.g., addEdge()) during iteration leads to undefined
     * behavior.
     *
     * @return An iterable of mutable out-edges
     */
    Iterable<MutableEdge<OID_T, EDATA_T>> getMutableEdges();

    /**
     * Return the value of the first edge with the given target vertex id, or null if there is no
     * such edge. Note: edge value objects returned by this method may be invalidated by the next
     * call. Thus, keeping a reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return EDATA_Tdge value (or null if missing)
     */
    EDATA_T getEdgeValue(long lid, OID_T targetVertexId);

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      EDATA_Tdge value
     */
    void setEdgeValue(OID_T targetVertexId, EDATA_T edgeValue);

    /**
     * Get an iterable over the values of all edges with the given target vertex id. This only makes
     * sense for multigraphs (i.e. graphs with parallel edges). Note: edge value objects returned by
     * this method may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return Iterable of edge values
     */
    Iterable<EDATA_T> getAllEdgeValues(OID_T targetVertexId);

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    public void addEdge(Edge<OID_T, EDATA_T> edge);

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    void removeEdges(OID_T targetVertexId);

    /**
     * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator, copy any remaining
     * edges to the new {@link OutEdges} data structure and keep a direct reference to it (thus
     * discarding the wrapper). Called by the Giraph infrastructure after computation.
     */
    public void unwrapMutableEdges();
}
