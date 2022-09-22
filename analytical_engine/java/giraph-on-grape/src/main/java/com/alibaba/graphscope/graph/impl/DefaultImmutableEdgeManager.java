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
package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.AbstractEdgeManager;
import com.alibaba.graphscope.graph.GiraphEdgeManager;
import com.alibaba.graphscope.graph.GiraphVertexIdManager;
import com.alibaba.graphscope.graph.GrapeEdge;
import com.alibaba.graphscope.graph.VertexIdManager;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Defeault edge manager, which extract all edata and oids from grape before accessing them in
 * queries.
 *
 * @param <GRAPE_OID_T>
 * @param <GRAPE_VID_T>
 * @param <GRAPE_VDATA_T>
 * @param <GRAPE_EDATA_T>
 * @param <GIRAPH_OID_T>
 * @param <GIRAPH_EDATA_T>
 */
public class DefaultImmutableEdgeManager<
                GRAPE_OID_T,
                GRAPE_VID_T,
                GRAPE_VDATA_T,
                GRAPE_EDATA_T,
                GIRAPH_OID_T extends WritableComparable,
                GIRAPH_EDATA_T extends Writable>
        extends AbstractEdgeManager<
                GRAPE_VID_T, GRAPE_OID_T, GIRAPH_OID_T, GRAPE_EDATA_T, GIRAPH_EDATA_T>
        implements GiraphEdgeManager<GIRAPH_OID_T, GIRAPH_EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(DefaultImmutableEdgeManager.class);

    private ImmutableClassesGiraphConfiguration<? super GIRAPH_OID_T, ?, ? super GIRAPH_EDATA_T>
            conf;
    private GenericEdgeIterable giraphEdgeIterable;

    public DefaultImmutableEdgeManager(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            GiraphVertexIdManager<GRAPE_VID_T, ? extends GIRAPH_OID_T> idManager,
            ImmutableClassesGiraphConfiguration<?, ?, ?> configuration) {

        //        vertexIdManager = (GiraphVertexIdManager<GRAPE_VID_T, GIRAPH_OID_T>) idManager;
        this.conf =
                (ImmutableClassesGiraphConfiguration<
                                ? super GIRAPH_OID_T, ?, ? super GIRAPH_EDATA_T>)
                        configuration;
        init(
                fragment,
                (VertexIdManager<GRAPE_VID_T, GIRAPH_OID_T>) idManager,
                (Class<? extends GIRAPH_OID_T>) conf.getVertexIdClass(),
                (Class<? extends GRAPE_VID_T>) conf.getGrapeVidClass(),
                (Class<? extends GRAPE_EDATA_T>) conf.getGrapeEdataClass(),
                (Class<? extends GIRAPH_EDATA_T>) conf.getEdgeValueClass(),
                ((inputStream, edatas) -> {
                    int index2 = 0;
                    try {
                        while (inputStream.longAvailable() > 0) {
                            GIRAPH_EDATA_T edata =
                                    (GIRAPH_EDATA_T)
                                            ReflectionUtils.newInstance(conf.getEdgeValueClass());
                            edata.readFields(inputStream);
                            edatas.set(index2++, edata);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    logger.info("read edges num [{}] from stream ", index2);
                }));
        giraphEdgeIterable = new GenericEdgeIterable();
    }

    /**
     * Get the number of outgoing edges on a vertex with its lid.
     *
     * @param lid
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges(long lid) {
        return getNumEdgesImpl(lid);
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
    public Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> getEdges(long lid) {
        //        edgeIterable.setLid(lid);
        //        return edgeIterable;
        giraphEdgeIterable.setLid(lid);
        return giraphEdgeIterable;
    }

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> edges) {
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
    public Iterable<MutableEdge<GIRAPH_OID_T, GIRAPH_EDATA_T>> getMutableEdges() {
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
    public GIRAPH_EDATA_T getEdgeValue(long lid, GIRAPH_OID_T targetVertexId) {
        Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> edges = getEdges(lid);
        for (Edge<GIRAPH_OID_T, GIRAPH_EDATA_T> edge : edges) {
            if (edge.getTargetVertexId().equals(targetVertexId)) {
                return edge.getValue();
            }
        }
        return null;
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      edge value
     */
    @Override
    public void setEdgeValue(GIRAPH_OID_T targetVertexId, GIRAPH_EDATA_T edgeValue) {
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
    public Iterable<GIRAPH_EDATA_T> getAllEdgeValues(GIRAPH_OID_T targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<GIRAPH_OID_T, GIRAPH_EDATA_T> edge) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(GIRAPH_OID_T targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void unwrapMutableEdges() {
        throw new IllegalStateException("not implemented");
    }

    public interface ImmutableEdgeIterator<
                    EDGE_OID_T extends WritableComparable, EDGE_DATA_T extends Writable>
            extends Iterator<Edge<EDGE_OID_T, EDGE_DATA_T>> {

        void setLid(int lid);
    }

    public class OnHeapEdgeIterator implements ImmutableEdgeIterator {
        private TupleIterator iterator;
        private DefaultEdge<GIRAPH_OID_T, GIRAPH_EDATA_T> edge = new DefaultEdge<>();

        public OnHeapEdgeIterator(TupleIterator tupleIterator) {
            this.iterator = tupleIterator;
        }

        @Override
        public void setLid(int lid) {
            iterator.setLid(lid);
        }

        /**
         * Returns {@code true} if the iteration has more elements. (In other words, returns {@code
         * true} if {@link #next} would return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         */
        @Override
        public Object next() {
            GrapeEdge<GRAPE_VID_T, GIRAPH_OID_T, GIRAPH_EDATA_T> grapeEdge = iterator.next();
            edge.setTargetVertexId(grapeEdge.dstOid);
            edge.setValue(grapeEdge.value);
            return edge;
        }
    }

    public class GenericEdgeIterable implements Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> {

        private ImmutableEdgeIterator iterator;

        public GenericEdgeIterable() {
            iterator = new OnHeapEdgeIterator((TupleIterator) edgeIterable.iterator());
        }

        public void setLid(long lid) {
            this.iterator.setLid((int) lid);
        }

        @Override
        public Iterator<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> iterator() {
            return iterator;
        }
    }
}
