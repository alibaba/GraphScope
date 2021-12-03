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

package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;

/**
 * EdgecutFragment defines the interfaces of fragments with edgecut. To learn more about edge-cut
 * and vertex-cut, please refers to
 * https://spark.apache.org/docs/1.6.2/graphx-programming-guide.html#optimized-representation
 *
 * <p>If we have an edge a-&lt;b cutted by the partitioner, and a is in frag_0, and b in frag_1.
 * Then:a-&lt;b is a crossing edge, a is an inner_vertex in frag_0, b is an outer_vertex in frag_0.
 *
 * @param <OID_T> original vertex id type.
 * @param <VID_T> vertex id type in GraphScope.
 * @param <VDATA_T> vertex data type.
 * @param <EDATA_T> edge data type.
 */
public interface EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T> {

    //    /** @return The number of bits for the fid offset. */
    //    int fid_offset();
    //
    //    /** @return The id mask used to mask global id into local id. */
    //    VID_T id_mask();
    /**
     * Get the number of inner vertices.
     *
     * @return number of inner vertices.
     */
    @FFINameAlias("GetInnerVerticesNum")
    @CXXValue
    VID_T getInnerVerticesNum();

    /**
     * Get the number of outer vertices.
     *
     * @return umber of outer vertices.
     */
    @FFINameAlias("GetOuterVerticesNum")
    @CXXValue
    VID_T getOuterVerticesNum();

    /**
     * Obtain vertex range contains all inner vertices.
     *
     * @return vertex range.
     */
    @FFINameAlias("InnerVertices")
    @CXXValue
    VertexRange<VID_T> innerVertices();

    /**
     * Obtain vertex range contains all outer vertices.
     *
     * @return vertex range.
     */
    @FFINameAlias("OuterVertices")
    @CXXValue
    VertexRange<VID_T> outerVertices();

    /**
     * Check whether a vertex is a inner vertex for a fragment.
     *
     * @param vertex querying vertex.
     * @return true if is inner vertex.
     */
    @FFINameAlias("IsInnerVertex")
    boolean isInnerVertex(@CXXReference Vertex<VID_T> vertex);

    /**
     * Check whether a vertex is a outer vertex for a fragment.
     *
     * @param vertex querying vertex.
     * @return true if is outer vertex.
     */
    @FFINameAlias("IsOuterVertex")
    boolean isOuterVertex(@CXXReference Vertex<VID_T> vertex);

    /**
     * Check whether a vertex, represented in OID_T, is a inner vertex. If yes, if true and put
     * inner representation id in the second param. Else return false.
     *
     * @param oid querying vertex in OID_T.
     * @param vertex placeholder for VID_T, if oid belongs to this fragment.
     * @return inner vertex or not.
     */
    @FFINameAlias("GetInnerVertex")
    boolean getInnerVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Check whether a vertex, represented in OID_T, is a outer vertex. If yes, if true and put
     * outer representation id in the second param. Else return false.
     *
     * @param oid querying vertex in OID_T.
     * @param vertex placeholder for VID_T, if oid doesn't belong to this fragment.
     * @return outer vertex or not.
     */
    @FFINameAlias("GetOuterVertex")
    boolean getOuterVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Obtain vertex id from original id, only for inner vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    @FFINameAlias("GetInnerVertexId")
    @CXXValue
    OID_T getInnerVertexId(@CXXReference Vertex<VID_T> vertex);

    /**
     * Obtain vertex id from original id, only for outer vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    @FFINameAlias("GetOuterVertexId")
    @CXXValue
    OID_T getOuterVertexId(@CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from global id to an inner vertex handle.
     *
     * @param gid Input global id.
     * @param vertex Output vertex handle.
     * @return True if exists an inner vertex of this fragment with global id as gid, false
     *     otherwise.
     */
    @FFINameAlias("InnerVertexGid2Vertex")
    boolean innerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from global id to an outer vertex handle.
     *
     * @param gid Input global id.
     * @param vertex Output vertex handle.
     * @return True if exists an outer vertex of this fragment with global id as gid, false
     *     otherwise.
     */
    @FFINameAlias("OuterVertexGid2Vertex")
    boolean outerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from inner vertex handle to its global id.
     *
     * @param vertex Input vertex handle.
     * @return Global id of the vertex.
     */
    @FFINameAlias("GetOuterVertexGid")
    @CXXValue
    VID_T getOuterVertexGid(@CXXReference Vertex<VID_T> vertex);

    /**
     * Convert from outer vertex handle to its global id.
     *
     * @param vertex Input vertex handle.
     * @return Global id of the vertex.
     */
    @FFINameAlias("GetInnerVertexGid")
    @CXXValue
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
    @FFINameAlias("IEDests")
    @CXXValue
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
    @FFINameAlias("OEDests")
    @CXXValue
    DestList outEdgeDests(@CXXReference Vertex<VID_T> vertex);

    /**
     * Get both the in edges and out edges.
     *
     * @param vertex query vertex.
     * @return The outgoing and incoming edge destination fragment ID list.
     */
    @FFINameAlias("IOEDests")
    @CXXValue
    DestList inOutEdgeDests(@CXXReference Vertex<VID_T> vertex);

    //    /**
    //     * Returns the incoming adjacent inner vertices of v.
    //     *
    //     * @param vertex querying vertex.
    //     * @return The incoming adjacent inner vertices of v.
    //     */
    //    @FFINameAlias("GetIncomingInnerVertexAdjList")
    //    @CXXValue
    //    GrapeAdjList<VID_T, EDATA_T> getIncomingInnerVertexAdjList(@CXXReference Vertex<VID_T>
    // vertex);
    //
    //    /**
    //     * Returns the outgoing adjacent inner vertices of v.
    //     *
    //     * @param vertex querying vertex.
    //     * @return The outgoing adjacent inner vertices of v.
    //     */
    //    @FFINameAlias("GetOutgoingInnerVertexAdjList")
    //    @CXXValue
    //    GrapeAdjList<VID_T, EDATA_T> getOutgoingInnerVertexAdjList(@CXXReference Vertex<VID_T>
    // vertex);
}
