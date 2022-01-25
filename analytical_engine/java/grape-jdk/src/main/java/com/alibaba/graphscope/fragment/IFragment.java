package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;

/**
 * IFragment defines a simple fragment interface, which conforms two different simple fragment
 * {@link ArrowProjectedFragment} and {@link ImmutableEdgecutFragment} into one.
 *
 * @param <OID_T> original vertex id type.
 * @param <VID_T> vertex id type.
 * @param <VDATA_T> vertex data type.
 * @param <EDATA_T> edge data type.
 */
public interface IFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    /**
     * Return the underlying fragment type,i.e. ArrowProjected or Simple.
     *
     * @return underlying fragment type.
     */
    String fragmentType();

    /**
     * Get the actual fragment FFIPointer we are using.
     *
     * @return a ffipointer
     */
    FFIPointer getFFIPointer();

    /** @return The id of current fragment. */
    int fid();

    /**
     * Number of fragments.
     *
     * @return number of fragments.
     */
    int fnum();

    /**
     * Returns the number of edges in this fragment.
     *
     * @return the number of edges in this fragment.
     */
    long getEdgeNum();

    /**
     * Returns the number of vertices in this fragment.
     *
     * @return the number of vertices in this fragment.
     */
    VID_T getVerticesNum();

    /**
     * Returns the number of vertices in the entire graph.
     *
     * @return The number of vertices in the entire graph.
     */
    long getTotalVerticesNum();

    /**
     * Get all vertices referenced to this fragment.
     *
     * @return A vertex set can be iterate on.
     */
    VertexRange<VID_T> vertices();

    /**
     * Get the vertex handle from the original id.
     *
     * @param oid input original id.
     * @param vertex output vertex handle
     * @return If find the vertex in this fragment, return true. Otherwise, return false.
     */
    boolean getVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Get the original Id of a vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    OID_T getId(@CXXReference Vertex<VID_T> vertex);

    /**
     * To which fragment the vertex belongs.
     *
     * @param vertex querying vertex.
     * @return frag id.
     */
    int getFragId(@CXXReference Vertex<VID_T> vertex);

    int getLocalInDegree(@CXXReference Vertex<VID_T> vertex);

    int getLocalOutDegree(@CXXReference Vertex<VID_T> vertex);

    boolean gid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    VID_T vertex2Gid(@CXXReference Vertex<VID_T> vertex);
    /**
     * Get the number of inner vertices.
     *
     * @return number of inner vertices.
     */
    long getInnerVerticesNum();

    /**
     * Get the number of outer vertices.
     *
     * @return umber of outer vertices.
     */
    long getOuterVerticesNum();

    /**
     * Obtain vertex range contains all inner vertices.
     *
     * @return vertex range.
     */
    VertexRange<VID_T> innerVertices();

    /**
     * Obtain vertex range contains all outer vertices.
     *
     * @return vertex range.
     */
    VertexRange<VID_T> outerVertices();

    /**
     * Check whether a vertex is a inner vertex for a fragment.
     *
     * @param vertex querying vertex.
     * @return true if is inner vertex.
     */
    boolean isInnerVertex(@CXXReference Vertex<VID_T> vertex);

    /**
     * Check whether a vertex is a outer vertex for a fragment.
     *
     * @param vertex querying vertex.
     * @return true if is outer vertex.
     */
    boolean isOuterVertex(@CXXReference Vertex<VID_T> vertex);

    /**
     * Check whether a vertex, represented in OID_T, is a inner vertex. If yes, if true and put
     * inner representation id in the second param. Else return false.
     *
     * @param oid querying vertex in OID_T.
     * @param vertex placeholder for VID_T, if oid belongs to this fragment.
     * @return inner vertex or not.
     */
    boolean getInnerVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Check whether a vertex, represented in OID_T, is a outer vertex. If yes, if true and put
     * outer representation id in the second param. Else return false.
     *
     * @param oid querying vertex in OID_T.
     * @param vertex placeholder for VID_T, if oid doesn't belong to this fragment.
     * @return outer vertex or not.
     */
    boolean getOuterVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Obtain vertex id from original id, only for inner vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    OID_T getInnerVertexId(@CXXReference Vertex<VID_T> vertex);

    /**
     * Obtain vertex id from original id, only for outer vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    OID_T getOuterVertexId(@CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from global id to an inner vertex handle.
     *
     * @param gid Input global id.
     * @param vertex Output vertex handle.
     * @return True if exists an inner vertex of this fragment with global id as gid, false
     *     otherwise.
     */
    boolean innerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from global id to an outer vertex handle.
     *
     * @param gid Input global id.
     * @param vertex Output vertex handle.
     * @return True if exists an outer vertex of this fragment with global id as gid, false
     *     otherwise.
     */
    boolean outerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from inner vertex handle to its global id.
     *
     * @param vertex Input vertex handle.
     * @return Global id of the vertex.
     */
    VID_T getOuterVertexGid(@CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from outer vertex handle to its global id.
     *
     * @param vertex Input vertex handle.
     * @return Global id of the vertex.
     */
    VID_T getInnerVertexGid(@CXXReference Vertex<VID_T> vertex);

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
    DestList inEdgeDests(@CXXReference Vertex<VID_T> vertex);

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
    DestList outEdgeDests(@CXXReference Vertex<VID_T> vertex);

    /**
     * Get both the in edges and out edges.
     *
     * @param vertex query vertex.
     * @return The outgoing and incoming edge destination fragment ID list.
     */
    DestList inOutEdgeDests(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue
    AdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue
    AdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @FFINameAlias("GetData")
    VDATA_T getData(@CXXReference Vertex<VID_T> vertex);

    /**
     * Update vertex data with a new value.
     *
     * @param vertex querying vertex.
     * @param vdata new vertex data.
     */
    @FFINameAlias("SetData")
    void setData(@CXXReference Vertex<VID_T> vertex, @CXXReference VDATA_T vdata);
}
