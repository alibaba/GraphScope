package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        implements IFragment<OID_T, VID_T, VDATA_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(ArrowProjectedAdaptor.class.getName());

    public static String fragmentType = "ArrowProjectedFragment";
    private ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment;

    public ArrowProjectedAdaptor(ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        fragment = frag;
    }

    public ArrowProjectedFragment getArrowProjectedFragment() {
        return fragment;
    }

    @Override
    public String fragmentType() {
        return fragmentType;
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
        logger.error("Method not implemented");
        return null;
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
