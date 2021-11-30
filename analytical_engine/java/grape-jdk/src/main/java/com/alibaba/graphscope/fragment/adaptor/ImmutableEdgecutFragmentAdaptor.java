package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.GrapeAdjListAdaptor;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.SimpleFragment;

public class ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        implements SimpleFragment<OID_T, VID_T, VDATA_T, EDATA_T> {
    public static String fragmentType = "ImmutableEdgecutFragment";
    private ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment;

    public ImmutableEdgecutFragmentAdaptor(
            ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        fragment = frag;
    }

    @Override
    public String fragmentType() {
        return fragmentType;
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
        return fragment.getLocalInDegree(vertex);
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
    public VID_T getInnerVerticesNum() {
        return fragment.getInnerVerticesNum();
    }

    @Override
    public VID_T getOuterVerticesNum() {
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
    public DestList inEdgeDests(Vertex<VID_T> vertex) {
        return inEdgeDests(vertex);
    }

    @Override
    public DestList outEdgeDests(Vertex<VID_T> vertex) {
        return outEdgeDests(vertex);
    }

    @Override
    public DestList inOutEdgeDests(Vertex<VID_T> vertex) {
        return inOutEdgeDests(vertex);
    }

    @Override
    public AdjList<VID_T, EDATA_T> getIncomingAdjList(Vertex<VID_T> vertex) {
        return new GrapeAdjListAdaptor<>(fragment.getIncomingAdjList(vertex));
    }

    @Override
    public AdjList<VID_T, EDATA_T> getOutgoingAdjList(Vertex<VID_T> vertex) {
        return new GrapeAdjListAdaptor<>(fragment.getOutgoingAdjList(vertex));
    }
}
