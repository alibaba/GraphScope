/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.FFIPointer;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.fragment.EdgecutFragment;
import com.alibaba.graphscope.fragment.IFragment;

public abstract class AbstractGraphXFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        implements IFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    private EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> edgecutFragment;

    public AbstractGraphXFragmentAdaptor(EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        this.edgecutFragment = fragment;
    }

    /**
     * Get the actual fragment FFIPointer we are using.
     *
     * @return a ffipointer
     */
    @Override
    public FFIPointer getFFIPointer() {
        return edgecutFragment;
    }

    /**
     * @return The id of current fragment.
     */
    @Override
    public int fid() {
        return edgecutFragment.fid();
    }

    /**
     * Number of fragments.
     *
     * @return number of fragments.
     */
    @Override
    public int fnum() {
        return edgecutFragment.fnum();
    }

    /**
     * Returns the number of edges in this fragment.
     *
     * @return the number of edges in this fragment.
     */
    @Override
    public long getEdgeNum() {
        return edgecutFragment.getEdgeNum();
    }

    /**
     * Returns the number of vertices in this fragment.
     *
     * @return the number of vertices in this fragment.
     */
    @Override
    public VID_T getVerticesNum() {
        return edgecutFragment.getVerticesNum();
    }

    /**
     * Returns the number of vertices in the entire graph.
     *
     * @return The number of vertices in the entire graph.
     */
    @Override
    public long getTotalVerticesNum() {
        return edgecutFragment.getTotalVerticesNum();
    }

    /**
     * Get all vertices referenced to this fragment.
     *
     * @return A vertex set can be iterate on.
     */
    @Override
    public VertexRange<VID_T> vertices() {
        return edgecutFragment.vertices();
    }

    /**
     * Get the vertex handle from the original id.
     *
     * @param oid    input original id.
     * @param vertex output vertex handle
     * @return If find the vertex in this fragment, return true. Otherwise, return false.
     */
    @Override
    public boolean getVertex(OID_T oid, Vertex<VID_T> vertex) {
        return edgecutFragment.getVertex(oid, vertex);
    }

    /**
     * Get the original Id of a vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    @Override
    public OID_T getId(Vertex<VID_T> vertex) {
        return edgecutFragment.getId(vertex);
    }

    /**
     * To which fragment the vertex belongs.
     *
     * @param vertex querying vertex.
     * @return frag id.
     */
    @Override
    public int getFragId(Vertex<VID_T> vertex) {
        return edgecutFragment.getFragId(vertex);
    }

    @Override
    public int getLocalInDegree(Vertex<VID_T> vertex) {
        return edgecutFragment.getLocalInDegree(vertex);
    }

    @Override
    public int getLocalOutDegree(Vertex<VID_T> vertex) {
        return edgecutFragment.getLocalOutDegree(vertex);
    }

    @Override
    public boolean gid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return edgecutFragment.gid2Vertex(gid, vertex);
    }

    @Override
    public VID_T vertex2Gid(Vertex<VID_T> vertex) {
        return edgecutFragment.vertex2Gid(vertex);
    }

    /**
     * Get the number of inner vertices.
     *
     * @return number of inner vertices.
     */
    @Override
    public long getInnerVerticesNum() {
        return edgecutFragment.getInnerVerticesNum();
    }

    /**
     * Get the number of outer vertices.
     *
     * @return umber of outer vertices.
     */
    @Override
    public long getOuterVerticesNum() {
        return edgecutFragment.getOuterVerticesNum();
    }

    /**
     * Obtain vertex range contains all inner vertices.
     *
     * @return vertex range.
     */
    @Override
    public VertexRange<VID_T> innerVertices() {
        return edgecutFragment.innerVertices();
    }

    /**
     * Obtain vertex range contains all outer vertices.
     *
     * @return vertex range.
     */
    @Override
    public VertexRange<VID_T> outerVertices() {
        return edgecutFragment.outerVertices();
    }

    /**
     * Check whether a vertex is a inner vertex for a fragment.
     *
     * @param vertex querying vertex.
     * @return true if is inner vertex.
     */
    @Override
    public boolean isInnerVertex(Vertex<VID_T> vertex) {
        return edgecutFragment.isInnerVertex(vertex);
    }

    /**
     * Check whether a vertex is a outer vertex for a fragment.
     *
     * @param vertex querying vertex.
     * @return true if is outer vertex.
     */
    @Override
    public boolean isOuterVertex(Vertex<VID_T> vertex) {
        return edgecutFragment.isOuterVertex(vertex);
    }

    /**
     * Check whether a vertex, represented in OID_T, is a inner vertex. If yes, if true and put
     * inner representation id in the second param. Else return false.
     *
     * @param oid    querying vertex in OID_T.
     * @param vertex placeholder for VID_T, if oid belongs to this fragment.
     * @return inner vertex or not.
     */
    @Override
    public boolean getInnerVertex(OID_T oid, Vertex<VID_T> vertex) {
        return edgecutFragment.getInnerVertex(oid, vertex);
    }

    /**
     * Check whether a vertex, represented in OID_T, is a outer vertex. If yes, if true and put
     * outer representation id in the second param. Else return false.
     *
     * @param oid    querying vertex in OID_T.
     * @param vertex placeholder for VID_T, if oid doesn't belong to this fragment.
     * @return outer vertex or not.
     */
    @Override
    public boolean getOuterVertex(OID_T oid, Vertex<VID_T> vertex) {
        return edgecutFragment.getOuterVertex(oid, vertex);
    }

    /**
     * Obtain vertex id from original id, only for inner vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    @Override
    public OID_T getInnerVertexId(Vertex<VID_T> vertex) {
        return edgecutFragment.getInnerVertexId(vertex);
    }

    /**
     * Obtain vertex id from original id, only for outer vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    @Override
    public OID_T getOuterVertexId(Vertex<VID_T> vertex) {
        return edgecutFragment.getOuterVertexId(vertex);
    }

    /**
     * Convert from global id to an inner vertex handle.
     *
     * @param gid    Input global id.
     * @param vertex Output vertex handle.
     * @return True if exists an inner vertex of this fragment with global id as gid, false
     * otherwise.
     */
    @Override
    public boolean innerVertexGid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return edgecutFragment.innerVertexGid2Vertex(gid, vertex);
    }

    /**
     * Convert from global id to an outer vertex handle.
     *
     * @param gid    Input global id.
     * @param vertex Output vertex handle.
     * @return True if exists an outer vertex of this fragment with global id as gid, false
     * otherwise.
     */
    @Override
    public boolean outerVertexGid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return edgecutFragment.outerVertexGid2Vertex(gid, vertex);
    }

    /**
     * Convert from inner vertex handle to its global id.
     *
     * @param vertex Input vertex handle.
     * @return Global id of the vertex.
     */
    @Override
    public VID_T getOuterVertexGid(Vertex<VID_T> vertex) {
        return edgecutFragment.getOuterVertexGid(vertex);
    }

    /**
     * Convert from outer vertex handle to its global id.
     *
     * @param vertex Input vertex handle.
     * @return Global id of the vertex.
     */
    @Override
    public VID_T getInnerVertexGid(Vertex<VID_T> vertex) {
        return edgecutFragment.getInnerVertexGid(vertex);
    }

    /**
     * Return the incoming edge destination fragment ID list of a inner vertex.
     *
     * <p>For inner vertex v of fragment-0, if outer vertex u and w are parents of v. u belongs to
     * fragment-1 and w belongs to fragment-2, then 1 and 2 are in incoming edge destination
     * fragment ID list of v.
     *
     * <p>This method is encapsulated in the corresponding sending message API,
     * SendMsgThroughIEdges, so it is not recommended to use this method directly in application
     * programs.
     *
     * @param vertex Input vertex.
     * @return The incoming edge destination fragment ID list.
     */

    /**
     * Return the outgoing edge destination fragment ID list of a inner vertex.
     *
     * <p>For inner vertex v of fragment-0, if outer vertex u and w are children of v. u belongs to
     * fragment-1 and w belongs to fragment-2, then 1 and 2 are in outgoing edge destination
     * fragment ID list of v.
     *
     * <p>This method is encapsulated in the corresponding sending message API,
     * SendMsgThroughOEdges, so it is not recommended to use this method directly in application
     * programs.
     *
     * @param vertex Input vertex.
     * @return The outgoing edge destination fragment ID list.
     */

    /**
     * Get both the in edges and out edges.
     *
     * @param vertex query vertex.
     * @return The outgoing and incoming edge destination fragment ID list.
     */
    @Override
    public AdjList<VID_T, EDATA_T> getIncomingAdjList(Vertex<VID_T> vertex) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public AdjList<VID_T, EDATA_T> getOutgoingAdjList(Vertex<VID_T> vertex) {
        throw new IllegalStateException("Not implemented");
    }

    /**
     * Update vertex data with a new value.
     *
     * @param vertex querying vertex.
     * @param vdata  new vertex data.
     */
    @Override
    public void setData(Vertex<VID_T> vertex, VDATA_T vdata) {
        throw new IllegalStateException("Not implemented");
    }
}
