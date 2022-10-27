package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.BaseArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        implements IFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    private static Logger logger =
            LoggerFactory.getLogger(AbstractArrowProjectedAdaptor.class.getName());

    private BaseArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> baseFragment;

    public AbstractArrowProjectedAdaptor(
            BaseArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        baseFragment = frag;
    }

    public BaseArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>
            getBaseArrayProjectedFragment() {
        return baseFragment;
    }

    /**
     * Get the actual fragment FFIPointer we are using.
     *
     * @return a ffipointer
     */
    @Override
    public FFIPointer getFFIPointer() {
        return baseFragment;
    }

    @Override
    public int fid() {
        return baseFragment.fid();
    }

    @Override
    public int fnum() {
        return baseFragment.fnum();
    }

    @Override
    public long getEdgeNum() {
        return baseFragment.getEdgeNum();
    }

    @Override
    public long getInEdgeNum() {
        return baseFragment.getIncomingEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return baseFragment.getOutgoingEdgeNum();
    }

    @Override
    public VID_T getVerticesNum() {
        return baseFragment.getVerticesNum();
    }

    @Override
    public long getTotalVerticesNum() {
        return baseFragment.getTotalVerticesNum();
    }

    @Override
    public VertexRange<VID_T> vertices() {
        return baseFragment.vertices();
    }

    @Override
    public boolean getVertex(OID_T oid, @CXXReference Vertex<VID_T> vertex) {
        return baseFragment.getVertex(oid, vertex);
    }

    @Override
    public OID_T getId(Vertex<VID_T> vertex) {
        return baseFragment.getId(vertex);
    }

    @Override
    public int getFragId(Vertex<VID_T> vertex) {
        return baseFragment.getFragId(vertex);
    }

    @Override
    public int getLocalInDegree(Vertex<VID_T> vertex) {
        return baseFragment.getLocalInDegree(vertex);
    }

    @Override
    public int getLocalOutDegree(Vertex<VID_T> vertex) {
        return baseFragment.getLocalOutDegree(vertex);
    }

    @Override
    public boolean gid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return baseFragment.gid2Vertex(gid, vertex);
    }

    @Override
    public VID_T vertex2Gid(Vertex<VID_T> vertex) {
        return baseFragment.vertex2Gid(vertex);
    }

    @Override
    public long getInnerVerticesNum() {
        return baseFragment.getInnerVerticesNum();
    }

    @Override
    public long getOuterVerticesNum() {
        return baseFragment.getOuterVerticesNum();
    }

    @Override
    public VertexRange<VID_T> innerVertices() {
        return baseFragment.innerVertices();
    }

    @Override
    public VertexRange<VID_T> outerVertices() {
        return baseFragment.outerVertices();
    }

    @Override
    public boolean isInnerVertex(Vertex<VID_T> vertex) {
        return baseFragment.isInnerVertex(vertex);
    }

    @Override
    public boolean isOuterVertex(Vertex<VID_T> vertex) {
        return baseFragment.isOuterVertex(vertex);
    }

    @Override
    public boolean getInnerVertex(OID_T oid, Vertex<VID_T> vertex) {
        return baseFragment.getInnerVertex(oid, vertex);
    }

    @Override
    public boolean getOuterVertex(OID_T oid, Vertex<VID_T> vertex) {
        return baseFragment.getOuterVertex(oid, vertex);
    }

    @Override
    public OID_T getInnerVertexId(Vertex<VID_T> vertex) {
        return baseFragment.getInnerVertexId(vertex);
    }

    @Override
    public OID_T getOuterVertexId(Vertex<VID_T> vertex) {
        return baseFragment.getOuterVertexId(vertex);
    }

    @Override
    public boolean innerVertexGid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return baseFragment.innerVertexGid2Vertex(gid, vertex);
    }

    @Override
    public boolean outerVertexGid2Vertex(VID_T gid, Vertex<VID_T> vertex) {
        return baseFragment.outerVertexGid2Vertex(gid, vertex);
    }

    @Override
    public VID_T getOuterVertexGid(Vertex<VID_T> vertex) {
        return baseFragment.getOuterVertexGid(vertex);
    }

    @Override
    public VID_T getInnerVertexGid(Vertex<VID_T> vertex) {
        return baseFragment.getInnerVertexGid(vertex);
    }

    /**
     * Update vertex data with a new value.
     *
     * @param vertex querying vertex.
     * @param vdata  new vertex data.
     */
    @Override
    public void setData(Vertex<VID_T> vertex, VDATA_T vdata) {
        logger.error("Method not implemented");
    }
}
