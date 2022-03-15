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

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;

import org.apache.commons.lang.NotImplementedException;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.MutableEdgesWrapper;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertexImpl<
                OID_T extends WritableComparable,
                VDATA_T extends Writable,
                EDATA_T extends Writable>
        extends DefaultImmutableClassesGiraphConfigurable<OID_T, VDATA_T, EDATA_T>
        implements Vertex<OID_T, VDATA_T, EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(VertexImpl.class);

    private long lid;

    private GiraphComputationAdaptorContext giraphComputationContext;

    private VertexDataManager<VDATA_T> vertexDataManager;
    private VertexIdManager<?, OID_T> vertexIdManager;
    /**
     * EdgeManager manages all vertex's edges.
     */
    private EdgeManager<OID_T, EDATA_T> edgeManager;

    // data set in initialization.
    private OID_T initializeOid;
    private VDATA_T initializeVdata;
    private Iterable<Edge<OID_T, EDATA_T>> initializeEdges;

    /**
     * Usually the default constructor is only used when we load vertex with vif.
     */
    public VertexImpl() {
        lid = -1;
    }

    public VertexImpl(GiraphComputationAdaptorContext ctx) {
        lid = -1; // set to a negative value to ensure set lid to be called later.
        this.giraphComputationContext = ctx;
    }

    public VertexDataManager getVertexDataManager() {
        return this.vertexDataManager;
    }

    public void setVertexDataManager(VertexDataManager vertexDataManager) {
        this.vertexDataManager = vertexDataManager;
    }

    public VertexIdManager getVertexIdManager() {
        return this.vertexIdManager;
    }

    public void setVertexIdManager(VertexIdManager vertexIdManager) {
        this.vertexIdManager = vertexIdManager;
    }

    public EdgeManager<OID_T, EDATA_T> getEdgeManager() {
        return this.edgeManager;
    }

    public void setEdgeManager(EdgeManager<OID_T, EDATA_T> edgeManager) {
        this.edgeManager = edgeManager;
    }

    /**
     * Initialize id, value, and edges. This method (or the alternative form initialize(id, value))
     * must be called after instantiation, unless readFields() is called.
     *
     * @param id    Vertex id
     * @param value Vertex value
     * @param edges Iterable of edges
     */
    @Override
    public void initialize(OID_T id, VDATA_T value, Iterable<Edge<OID_T, EDATA_T>> edges) {
        initializeOid = id;
        initializeVdata = value;
        initializeEdges = edges;
    }

    /**
     * Initialize id and value. Vertex edges will be empty. This method (or the alternative form
     * initialize(id, value, edges)) must be called after instantiation, unless readFields() is
     * called.
     *
     * @param id    Vertex id
     * @param value Vertex value
     */
    @Override
    public void initialize(OID_T id, VDATA_T value) {
        initializeOid = id;
        initializeVdata = value;
    }

    /**
     * Get the vertex id.
     *
     * @return My vertex id.
     */
    @Override
    public OID_T getId() {
        return vertexIdManager.getId(lid);
    }

    /**
     * Get the vertex value (data stored with vertex)
     *
     * @return Vertex value
     */
    @Override
    public VDATA_T getValue() {
        return vertexDataManager.getVertexData(lid);
    }

    /**
     * Set the vertex data (immediately visible in the computation)
     *
     * @param value Vertex data to be set
     */
    @Override
    public void setValue(VDATA_T value) {
        vertexDataManager.setVertexData(lid, value);
    }

    /**
     * After this is called, the compute() code will no longer be called for this vertex unless a
     * message is sent to it. Then the compute() code will be called once again until this function
     * is called. The application finishes only when all vertices vote to halt.
     */
    @Override
    public void voteToHalt() {
        giraphComputationContext.haltVertex(lid);
    }

    /**
     * Get the number of outgoing edges on this vertex.
     *
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges() {
        return edgeManager.getNumEdges(lid);
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
    public Iterable<Edge<OID_T, EDATA_T>> getEdges() {
        return edgeManager.getEdges(lid);
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
     * @return Edge value (or null if missing)
     */
    @Override
    public EDATA_T getEdgeValue(OID_T targetVertexId) {
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      Edge value
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
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<OID_T, EDATA_T> edge) {
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(OID_T targetVertexId) {
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /**
     * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator, copy any remaining
     * edges to the new {@link OutEdges} data structure and keep a direct reference to it (thus
     * discarding the wrapper). Called by the Giraph infrastructure after computation.
     */
    @Override
    public void unwrapMutableEdges() {
        logger.error("Not implemented");
        throw new NotImplementedException();
    }

    /**
     * Re-activate vertex if halted.
     */
    @Override
    public void wakeUp() {
        giraphComputationContext.activateVertex(lid);
    }

    /**
     * Is this vertex done?
     *
     * @return True if halted, false otherwise.
     */
    @Override
    public boolean isHalted() {
        return giraphComputationContext.isHalted(lid);
    }

    public long getLocalId() {
        return lid;
    }

    // Methods we need to adapt to grape
    public void setLocalId(int lid) {
        this.lid = lid;
    }

    @Override
    public void forceContinue() {
        giraphComputationContext.getGiraphMessageManager().forceContinue();
    }
}
