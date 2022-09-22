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

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.IFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        implements IFragment<OID_T, VID_T, VDATA_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(ArrowProjectedAdaptor.class.getName());

    private ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment;

    @Override
    public String toString() {
        return "ArrowProjectedAdaptor{" + "fragment=" + fragment + '}';
    }

    public ArrowProjectedAdaptor(ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        fragment = frag;
    }

    public ArrowProjectedFragment getArrowProjectedFragment() {
        return fragment;
    }

    @Override
    public FragmentType fragmentType() {
        return FragmentType.ArrowProjectedFragment;
    }

    /**
     * Get the actual fragment FFIPointer we are using.
     *
     * @return a ffipointer
     */
    @Override
    public FFIPointer getFFIPointer() {
        return fragment;
    }

    @Override
    public int fid() {
        return fragment.fid();
    }

    @Override
    public int fnum() {
        return fragment.fnum();
    }

    @Override
    public long getEdgeNum() {
        return fragment.getEdgeNum();
    }

    @Override
    public long getInEdgeNum() {
        return fragment.getInEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return fragment.getOutEdgeNum();
    }

    @Override
    public VID_T getVerticesNum() {
        return fragment.getVerticesNum();
    }

    @Override
    public long getTotalVerticesNum() {
        return fragment.getTotalVerticesNum();
    }

    @Override
    public VertexRange<VID_T> vertices() {
        return fragment.vertices();
    }

    @Override
    public boolean getVertex(OID_T oid, @CXXReference Vertex<VID_T> vertex) {
        return fragment.getVertex(oid, vertex);
    }

    @Override
    public OID_T getId(Vertex<VID_T> vertex) {
        return fragment.getId(vertex);
    }

    @Override
    public int getFragId(Vertex<VID_T> vertex) {
        return fragment.getFragId(vertex);
    }

    @Override
    public int getLocalInDegree(Vertex<VID_T> vertex) {
        return fragment.getLocalInDegree(vertex);
    }

    @Override
    public int getLocalOutDegree(Vertex<VID_T> vertex) {
        return fragment.getLocalOutDegree(vertex);
    }

    @Override
    public boolean gid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return fragment.gid2Vertex(gid, vertex);
    }

    @Override
    public VID_T vertex2Gid(Vertex<VID_T> vertex) {
        return fragment.vertex2Gid(vertex);
    }

    @Override
    public long getInnerVerticesNum() {
        return fragment.getInnerVerticesNum();
    }

    @Override
    public long getOuterVerticesNum() {
        return fragment.getOuterVerticesNum();
    }

    @Override
    public VertexRange<VID_T> innerVertices() {
        return fragment.innerVertices();
    }

    @Override
    public VertexRange<VID_T> outerVertices() {
        return fragment.outerVertices();
    }

    @Override
    public boolean isInnerVertex(Vertex<VID_T> vertex) {
        return fragment.isInnerVertex(vertex);
    }

    @Override
    public boolean isOuterVertex(Vertex<VID_T> vertex) {
        return fragment.isOuterVertex(vertex);
    }

    @Override
    public boolean getInnerVertex(OID_T oid, Vertex<VID_T> vertex) {
        return fragment.getInnerVertex(oid, vertex);
    }

    @Override
    public boolean getOuterVertex(OID_T oid, Vertex<VID_T> vertex) {
        return fragment.getOuterVertex(oid, vertex);
    }

    @Override
    public OID_T getInnerVertexId(Vertex<VID_T> vertex) {
        return fragment.getInnerVertexId(vertex);
    }

    @Override
    public OID_T getOuterVertexId(Vertex<VID_T> vertex) {
        return fragment.getOuterVertexId(vertex);
    }

    @Override
    public boolean innerVertexGid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return fragment.innerVertexGid2Vertex(gid, vertex);
    }

    @Override
    public boolean outerVertexGid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return fragment.outerVertexGid2Vertex(gid, vertex);
    }

    @Override
    public VID_T getOuterVertexGid(Vertex<VID_T> vertex) {
        return fragment.getOuterVertexGid(vertex);
    }

    @Override
    public VID_T getInnerVertexGid(Vertex<VID_T> vertex) {
        return fragment.getInnerVertexGid(vertex);
    }

    @Override
    public AdjList<VID_T, EDATA_T> getIncomingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListAdaptor<>(fragment.getIncomingAdjList(vertex));
    }

    @Override
    public AdjList<VID_T, EDATA_T> getOutgoingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListAdaptor<>(fragment.getOutgoingAdjList(vertex));
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public VDATA_T getData(Vertex<VID_T> vertex) {
        //        throw new IllegalStateException("Not implemented");
        return fragment.getData(vertex);
    }

    /**
     * Update vertex data with a new value.
     *
     * @param vertex querying vertex.
     * @param vdata new vertex data.
     */
    @Override
    public void setData(Vertex<VID_T> vertex, VDATA_T vdata) {
        logger.error("Method not implemented");
    }
}
