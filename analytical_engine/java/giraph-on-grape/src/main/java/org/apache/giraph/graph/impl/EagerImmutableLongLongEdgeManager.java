/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.NbrBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.LongLongEdge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Immutable edges manager specialized for long oid and long edata. Store edges as n maps. each
 * maps
 *
 * @param <GRAPE_OID_T>>
 * @param <GRAPE_VID_T>
 * @param <GRAPE_VDATA_T>
 * @param <GRAPE_EDATA_T>
 */
public class EagerImmutableLongLongEdgeManager<
                GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
        implements EdgeManager<LongWritable, LongWritable> {

    private ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ? super LongWritable> conf;
    private VertexIdManager<GRAPE_VID_T, LongWritable> vertexIdManager;
    private Vertex<GRAPE_VID_T> grapeVertex;
    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private Long2ObjectArrayMap edgesTargetIdsMap;
    private Long2ObjectArrayMap edgesValuesMap;
    private LongLongEdgeIterable edgeIterable = new LongLongEdgeIterable();

    public EagerImmutableLongLongEdgeManager(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            VertexIdManager<GRAPE_VID_T, ? extends LongWritable> idManager,
            ImmutableClassesGiraphConfiguration<?, ?, ?> configuration) {
        this.fragment = fragment;
        grapeVertex =
                (Vertex<GRAPE_VID_T>)
                        FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
        vertexIdManager = (VertexIdManager<GRAPE_VID_T, LongWritable>) idManager;
        this.conf =
                (ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ? super LongWritable>)
                        configuration;

        if (!conf.getGrapeOidClass().equals(Long.class)
                || !conf.getGrapeEdataClass().equals(Long.class)) {
            throw new IllegalStateException(
                    "Inconsistency in long long edge manager "
                            + conf.getGrapeOidClass().getSimpleName()
                            + ", "
                            + conf.getGrapeEdataClass().getSimpleName());
        }
        edgesTargetIdsMap = new Long2ObjectArrayMap((int) fragment.getInnerVerticesNum());
        edgesValuesMap = new Long2ObjectArrayMap((int) fragment.getInnerVerticesNum());
        // Copy all edges to java memory has too much cost, don't copy
        for (Long lid = 0L; lid < fragment.getInnerVerticesNum(); ++lid) {
            grapeVertex.SetValue((GRAPE_VID_T) lid);
            AdjList<GRAPE_VID_T, GRAPE_EDATA_T> adjList = fragment.getOutgoingAdjList(grapeVertex);

            // TODO: make this faster
            List<Long> targetIds = new ArrayList<Long>((int) adjList.size());
            List<Long> edgeValues = new ArrayList<Long>((int) adjList.size());
            for (NbrBase<GRAPE_VID_T, GRAPE_EDATA_T> nbr : adjList.nbrBases()) {
                targetIds.add((Long) nbr.neighbor().GetValue());
                edgeValues.add((Long) nbr.data());
            }
            edgesTargetIdsMap.put(lid, targetIds);
            edgesValuesMap.put(lid, edgeValues);
        }
    }

    /**
     * Get the number of outgoing edges on a vertex with its lid.
     *
     * @param lid
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges(long lid) {
        //        grapeVertex.SetValue((GRAPE_VID_T) conf.getGrapeVidClass().cast(lid));
        //        return (int) (fragment.getOutgoingAdjList(grapeVertex).size());
        return ((List<Long>) edgesTargetIdsMap.get(lid)).size();
    }

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEdge()) during iteration leads to undefined behavior.
     *
     * @param lid
     * @return the out edges (sort order determined by subclass implementation).
     */
    @Override
    public Iterable<Edge<LongWritable, LongWritable>> getEdges(long lid) {
        edgeIterable.setSourceLid(lid);
        return edgeIterable;
    }

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<LongWritable, LongWritable>> edges) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Get an iterable of out-edges that can be modified in-place. This can mean changing the
     * current edge value or removing the current edge (by using the iterator version). Note:
     * accessing the edges with other methods (e.g., addEdge()) during iteration leads to undefined
     * behavior.
     *
     * @return An iterable of mutable out-edges
     */
    @Override
    public Iterable<MutableEdge<LongWritable, LongWritable>> getMutableEdges() {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Return the value of the first edge with the given target vertex id, or null if there is no
     * such edge. Note: edge value objects returned by this method may be invalidated by the next
     * call. Thus, keeping a reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return edge value (or null if missing)
     */
    @Override
    public LongWritable getEdgeValue(LongWritable targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      edge value
     */
    @Override
    public void setEdgeValue(LongWritable targetVertexId, LongWritable edgeValue) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Get an iterable over the values of all edges with the given target vertex id. This only makes
     * sense for multigraphs (i.e. graphs with parallel edges). Note: edge value objects returned by
     * this method may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return Iterable of edge values
     */
    @Override
    public Iterable<LongWritable> getAllEdgeValues(LongWritable targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<LongWritable, LongWritable> edge) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(LongWritable targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator, copy any remaining
     * edges to the new {@link OutEdges} data structure and keep a direct reference to it (thus
     * discarding the wrapper). Called by the Giraph infrastructure after computation.
     */
    @Override
    public void unwrapMutableEdges() {
        throw new IllegalStateException("not implemented");
    }

    public class LongLongEdgeIterator implements Iterator<Edge<LongWritable, LongWritable>> {

        private LongLongEdge edge = new LongLongEdge();
        private Iterator<Long> targetIdsIterator;
        private Iterator<Long> edgesValuesIterator;

        public void setSourceLid(long lid) {
            targetIdsIterator = ((List<Long>) edgesTargetIdsMap.get(lid)).iterator();
            edgesValuesIterator = ((List<Long>) edgesValuesMap.get(lid)).iterator();
        }

        @Override
        public boolean hasNext() {
            return targetIdsIterator.hasNext();
        }

        @Override
        public Edge<LongWritable, LongWritable> next() {
            edge.setTargetVertexId(vertexIdManager.getId(targetIdsIterator.next()));
            edge.setValue(edgesValuesIterator.next());
            return edge;
        }
    }

    public class LongLongEdgeIterable implements Iterable<Edge<LongWritable, LongWritable>> {

        private LongLongEdgeIterator iterator;

        // made private to ensure singleton.
        private LongLongEdgeIterable() {
            iterator = new LongLongEdgeIterator();
        }

        public void setSourceLid(long lid) {
            iterator.setSourceLid(lid);
        }

        /**
         * Returns an iterator over elements of type {@code T}.
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<Edge<LongWritable, LongWritable>> iterator() {
            return iterator;
        }
    }
}
