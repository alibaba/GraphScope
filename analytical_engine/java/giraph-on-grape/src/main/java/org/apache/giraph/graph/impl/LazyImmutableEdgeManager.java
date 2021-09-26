package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.NbrBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.apache.commons.lang.NotImplementedException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Edge management for immutable EdgeCut fragment. In all time, there shall be only one
 * EdgeManagerImpl, but multiple iterables.
 *
 * This is lazy because it only copy edges from c++ to java when needed.
 *
 * @param <OID_T> giraph vertex id type
 * @param <EDATA_T> giraph edge data type
 * @param <GRAPE_OID_T> grape fragment oid type
 * @param <GRAPE_VID_T> grape fragment vid type
 * @param <GRAPE_VDATA_T> grape vdata type
 * @param <GRAPE_EDATA_T> grape edge data type
 */
public class LazyImmutableEdgeManager<
                OID_T extends WritableComparable,
                EDATA_T extends Writable,
                GRAPE_OID_T,
                GRAPE_VID_T,
                GRAPE_VDATA_T,
                GRAPE_EDATA_T>
        implements EdgeManager<OID_T, EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(LazyImmutableEdgeManager.class);

    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, EDATA_T> configuration;
    /** Grape store edge by (lid, edata) we need a converter. */
    private VertexIdManager<OID_T> vertexIdManager;

    private Vertex<GRAPE_VID_T> grapeVertex;

    public LazyImmutableEdgeManager(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            VertexIdManager<OID_T> idManager,
            ImmutableClassesGiraphConfiguration<OID_T, ?, EDATA_T> configuration) {
        this.fragment = fragment;
        this.vertexIdManager = idManager;
        this.configuration = configuration;
        grapeVertex =
                (Vertex<GRAPE_VID_T>)
                        FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
    }

    /**
     * Get the number of outgoing edges on this vertex.
     *
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges(long lid) {
        if (configuration.getGrapeVidClass().equals(Long.class)) {
            grapeVertex.SetValue((GRAPE_VID_T) configuration.getGrapeVidClass().cast(lid));
        } else if (configuration.getGrapeVidClass().equals(Integer.class)) {
            grapeVertex.SetValue((GRAPE_VID_T) configuration.getGrapeVidClass().cast((int) lid));
        } else {
            logger.error("grape vid class can only be long or integer");
            throw new IllegalStateException("grape vid class can only be long or integer");
        }
        //        return (int) (fragment.getOutgoingAdjList(grapeVertex).size() +
        // fragment.getIncomingAdjList(
        //            grapeVertex).size());
        return (int) (fragment.getOutgoingAdjList(grapeVertex).size());
    }

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEdge()) during iteration leads to undefined behavior.
     *
     * @return the out edges (sort order determined by subclass implementation).
     */
    @Override
    public Iterable<Edge<OID_T, EDATA_T>> getEdges(long lid) {
        if (configuration.getGrapeVidClass().equals(Long.class)) {
            return new LongEdgeIterable(lid);
        } else if (configuration.getGrapeVidClass().equals(Integer.class)) {
            return new IntEdgeIterable((int) lid);
        } else {
            throw new IllegalStateException("Oid should be int or long");
        }
    }

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<OID_T, EDATA_T>> edges) {
        logger.error("Not implemented");
        throw new NotImplementedException();
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
    public Iterable<MutableEdge<OID_T, EDATA_T>> getMutableEdges() {
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /**
     * Return the value of the first edge with the given target vertex id, or null if there is no
     * such edge. Note: edge value objects returned by this method may be invalidated by the next
     * call. Thus, keeping a reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return EDATA_Tdge value (or null if missing)
     */
    @Override
    public EDATA_T getEdgeValue(OID_T targetVertexId) {
        throw new NotImplementedException();
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue EDATA_Tdge value
     */
    @Override
    public void setEdgeValue(OID_T targetVertexId, EDATA_T edgeValue) {
        logger.error("Not implemented");
        throw new NotImplementedException();
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
    public Iterable<EDATA_T> getAllEdgeValues(OID_T targetVertexId) {
        throw new NotImplementedException();
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<OID_T, EDATA_T> edge) {
        throw new NotImplementedException();
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(OID_T targetVertexId) {
        throw new NotImplementedException();
    }

    //    /**
    //     * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator, copy any
    // remaining
    //     * edges to the new {@link OutEdges} data structure and keep a direct reference to it
    // (thus
    //     * discarding the wrapper). Called by the Giraph infrastructure after computation.
    //     */
    @Override
    public void unwrapMutableEdges() {
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /** Iterable for edges from one vertex. oid=long */
    public class LongEdgeIterable implements Iterable<Edge<OID_T, EDATA_T>> {

        private long lid;
        private Vertex<GRAPE_VID_T> vertex;

        public LongEdgeIterable(long lid) {
            this.lid = lid;
            vertex =
                    (Vertex<GRAPE_VID_T>)
                            FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
            // It is safe to cast Integer to Long, but not vice-verse.
            vertex.SetValue((GRAPE_VID_T) configuration.getGrapeVidClass().cast(this.lid));
        }

        /**
         * Returns an iterator over elements of type {@code T}. Make sure lid is updated before
         * calling iterator()
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<Edge<OID_T, EDATA_T>> iterator() {
            return new LongNbrIterator(vertex);
            //            if (configuration.getGrapeVidClass().equals(Long.class)){
            //                return new LongNbrIterator(vertex);
            //            }
            //            else if (configuration.getGrapeVidClass().equals(Integer.class)){
            //                return new IntNbrIterator(vertex);
            //            }
            //            logger.error("Only accept long and int as vid class");
            //            return null;
        }
    }

    /** Iterable for edges from one vertex. oid=int */
    public class IntEdgeIterable implements Iterable<Edge<OID_T, EDATA_T>> {

        private int lid;
        private Vertex<GRAPE_VID_T> vertex;

        public IntEdgeIterable(int lid) {
            this.lid = lid;
            vertex =
                    (Vertex<GRAPE_VID_T>)
                            FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
            // It is safe to cast Integer to Long, but not vice-verse.
            vertex.SetValue((GRAPE_VID_T) configuration.getGrapeVidClass().cast(this.lid));
        }

        /**
         * Returns an iterator over elements of type {@code T}. Make sure lid is updated before
         * calling iterator()
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<Edge<OID_T, EDATA_T>> iterator() {
            return new IntNbrIterator(vertex);
            //            if (configuration.getGrapeVidClass().equals(Long.class)){
            //                return new LongNbrIterator(vertex);
            //            }
            //            else if (configuration.getGrapeVidClass().equals(Integer.class)){
            //                return new IntNbrIterator(vertex);
            //            }
            //            logger.error("Only accept long and int as vid class");
            //            return null;
        }
    }

    /** Specialized iterator for vid=long64 */
    public class LongNbrIterator implements Iterator<Edge<OID_T, EDATA_T>> {

        private ReusableEdge<OID_T, EDATA_T> edge;
        private Iterator<? extends NbrBase<GRAPE_VID_T, GRAPE_EDATA_T>> nbrIterator;
        private NbrBase<GRAPE_VID_T, GRAPE_EDATA_T> nbr;
        private FFIByteVectorInputStream inputStream;

        /**
         * When init a nbr iterator, We preload all edge data into a stream, then we sequentially
         * read from it.
         *
         * @param vertex whose neighbors we want
         */
        public LongNbrIterator(Vertex<GRAPE_VID_T> vertex) {
            this.edge = new DefaultEdge<>();
            AdjList<GRAPE_VID_T, GRAPE_EDATA_T> adjList = fragment.getOutgoingAdjList(vertex);
            this.nbrIterator = adjList.nbrBases().iterator();
            edge.setTargetVertexId(configuration.createVertexId());
            edge.setValue(configuration.createEdgeValue());

            this.inputStream = generateEdataInputStream(adjList);
        }

        @Override
        public boolean hasNext() {
            return nbrIterator.hasNext();
        }

        @Override
        public Edge<OID_T, EDATA_T> next() {
            nbr = nbrIterator.next();
            long neighborLid = (Long) nbr.neighbor().GetValue();
            OID_T oid = vertexIdManager.getId(neighborLid);
            edge.setTargetVertexId(oid);
            try {
                // Try catch introduces no performance cost.
                edge.getValue().readFields(this.inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return edge;
        }
    }

    /** Specialized iterator for vid=int32 */
    public class IntNbrIterator implements Iterator<Edge<OID_T, EDATA_T>> {

        private ReusableEdge<OID_T, EDATA_T> edge;
        private Iterator<? extends NbrBase<GRAPE_VID_T, GRAPE_EDATA_T>> nbrIterator;
        private NbrBase<GRAPE_VID_T, GRAPE_EDATA_T> nbr;
        private FFIByteVectorInputStream inputStream;

        /**
         * When init a nbr iterator, We preload all edge data into a stream, then we sequentially
         * read from it.
         *
         * @param vertex whose neighbors we want
         */
        public IntNbrIterator(Vertex<GRAPE_VID_T> vertex) {
            this.edge = new DefaultEdge<>();
            AdjList<GRAPE_VID_T, GRAPE_EDATA_T> adjList = fragment.getOutgoingAdjList(vertex);
            this.nbrIterator = adjList.nbrBases().iterator();
            edge.setTargetVertexId((OID_T) configuration.createVertexId());
            edge.setValue((EDATA_T) configuration.createEdgeValue());

            this.inputStream = generateEdataInputStream(adjList);
        }

        @Override
        public boolean hasNext() {
            return nbrIterator.hasNext();
        }

        @Override
        public Edge<OID_T, EDATA_T> next() {
            nbr = nbrIterator.next();
            int neighborLid = (Integer) nbr.neighbor().GetValue();
            OID_T oid = vertexIdManager.getId(neighborLid);
            edge.setTargetVertexId(oid);
            try {
                // Try catch introduces no performance cost.
                edge.getValue().readFields(this.inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return edge;
        }
    }

    private FFIByteVectorInputStream generateEdataInputStream(
            AdjList<GRAPE_VID_T, GRAPE_EDATA_T> adjList) {
        FFIByteVectorOutputStream stream = new FFIByteVectorOutputStream();
        try {
            if (configuration.getGrapeEdataClass().equals(Long.class)) {
                for (Nbr<GRAPE_VID_T, GRAPE_EDATA_T> tmpNbr : adjList.iterable()) {
                    stream.writeLong((Long) tmpNbr.data());
                }
            } else if (configuration.getGrapeEdataClass().equals(Double.class)) {
                for (Nbr<GRAPE_VID_T, GRAPE_EDATA_T> tmpNbr : adjList.iterable()) {
                    stream.writeDouble((Double) tmpNbr.data());
                }
            } else if (configuration.getGrapeEdataClass().equals(Float.class)) {
                for (Nbr<GRAPE_VID_T, GRAPE_EDATA_T> tmpNbr : adjList.iterable()) {
                    stream.writeDouble((Float) tmpNbr.data());
                }
            } else if (configuration.getGrapeEdataClass().equals(Integer.class)) {
                for (Nbr<GRAPE_VID_T, GRAPE_EDATA_T> tmpNbr : adjList.iterable()) {
                    stream.writeInt((Integer) tmpNbr.data());
                }
            }
            // TODO: for user defined edge data.
            else {
                logger.error(
                        "Edata class: "
                                + configuration.getGrapeEdataClass().getName()
                                + "Not supported now");
            }
        } catch (Exception e) {
            e.printStackTrace();
            stream.getVector().clear();
            return null;
        }
        return new FFIByteVectorInputStream(stream.getVector());
    }
}
