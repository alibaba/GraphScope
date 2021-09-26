package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.NbrBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.Iterator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.LongLongEdge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.LongWritable;

/**
 * A Immutable edges manager specialized for long oid and long edata. Store edges as n maps. each
 * maps
 *
 * @param <GRAPE_OID_T>>
 * @param <GRAPE_VID_T>
 * @param <GRAPE_VDATA_T>
 * @param <GRAPE_EDATA_T>
 */
public class LazyImmutableLongLongEdgeManager<
    GRAPE_OID_T,
    GRAPE_VID_T,
    GRAPE_VDATA_T,
    GRAPE_EDATA_T>
    implements EdgeManager<LongWritable, LongWritable> {

    private ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ? super LongWritable> conf;
    private VertexIdManager<LongWritable> vertexIdManager;
    private Vertex<GRAPE_VID_T> grapeVertex;
    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;

    private LongLongEdgeIterable edgeIterable = new LongLongEdgeIterable();

    public LazyImmutableLongLongEdgeManager(
        IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
        VertexIdManager<? extends LongWritable> idManager,
        ImmutableClassesGiraphConfiguration<?, ?, ?> configuration) {
        this.fragment = fragment;
        grapeVertex =
            (Vertex<GRAPE_VID_T>)
                FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
        vertexIdManager = (VertexIdManager<LongWritable>) idManager;
        this.conf = (ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ? super LongWritable>) configuration;

        if (!conf.getGrapeOidClass().equals(Long.class) || !conf.getGrapeEdataClass()
            .equals(Long.class)) {
            throw new IllegalStateException(
                "Inconsistency in long long edge manager " + conf.getGrapeOidClass().getSimpleName()
                    + ", " + conf.getGrapeEdataClass().getSimpleName());
        }
        //Copy all edges to java memory has too much cost, don't copy

    }

    /**
     * Get the number of outgoing edges on a vertex with its lid.
     *
     * @param lid
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges(long lid) {
        grapeVertex.SetValue((GRAPE_VID_T) conf.getGrapeVidClass().cast(lid));
        return (int) (fragment.getOutgoingAdjList(grapeVertex).size());
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
        edgeIterable.setSourceId(lid);
        return edgeIterable;
    }

    public class LongLongEdgeIterator implements Iterator<Edge<LongWritable, LongWritable>> {
        private Iterator<? extends NbrBase<Long, Long>> nbrIterator;
        private LongLongEdge edge;

        public void setNbrIterator(Iterator<? extends NbrBase<Long, Long>> adjList) {
            this.nbrIterator = adjList;
            this.edge = new LongLongEdge();
        }

        @Override
        public boolean hasNext() {
            return nbrIterator.hasNext();
        }

        @Override
        public Edge<LongWritable, LongWritable> next() {
            NbrBase<Long, Long> nbrBase = nbrIterator.next();
            edge.setTargetVertexId(
                vertexIdManager.getId(nbrBase.neighbor().GetValue().longValue()));
            edge.setValue(nbrBase.data());
            return edge;
        }
    }

    public class LongLongEdgeIterable implements Iterable<Edge<LongWritable, LongWritable>> {

        private LongLongEdgeIterator iterator;
        //made private to ensure singleton.
        private LongLongEdgeIterable() {
            iterator = new LongLongEdgeIterator();
        }

        /**
         * Reuse this iterable by set sourceId.
         *
         * @param lid source lid.
         */
        public void setSourceId(long lid) {
            grapeVertex.SetValue((GRAPE_VID_T) conf.getGrapeVidClass().cast(lid));
            iterator.setNbrIterator(
                (Iterator<? extends NbrBase<Long, Long>>) fragment.getOutgoingAdjList(grapeVertex).nbrBases().iterator());
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
}
