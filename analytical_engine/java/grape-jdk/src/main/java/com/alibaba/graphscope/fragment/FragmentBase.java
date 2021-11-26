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
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;

/**
 * Defines the basic interfaces shall be provided by a fragment.
 *
 * @param <OID_T> original vertex id type.
 * @param <VID_T> vertex id type.
 * @param <VDATA_T> vertex data type.
 * @param <EDATA_T> edge data type.
 */
public interface FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T> extends FFIPointer {

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
    @FFINameAlias("GetEdgeNum")
    long getEdgeNum();

    /**
     * Returns the number of vertices in this fragment.
     *
     * @return the number of vertices in this fragment.
     */
    @FFINameAlias("GetVerticesNum")
    VID_T getVerticesNum();

    /**
     * Returns the number of vertices in the entire graph.
     *
     * @return The number of vertices in the entire graph.
     */
    @FFINameAlias("GetTotalVerticesNum")
    long getTotalVerticesNum();

    /**
     * Get all vertices referenced to this fragment.
     *
     * @return A vertex set can be iterate on.
     */
    @FFINameAlias("Vertices")
    @CXXValue
    VertexRange<VID_T> vertices();

    /**
     * Get the vertex handle from the original id.
     *
     * @param oid input original id.
     * @param vertex output vertex handle
     * @return If find the vertex in this fragment, return true. Otherwise, return false.
     */
    @FFINameAlias("GetVertex")
    boolean getVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Get the original Id of a vertex.
     *
     * @param vertex querying vertex.
     * @return original id.
     */
    @FFINameAlias("GetId")
    @CXXValue
    OID_T getId(@CXXReference Vertex<VID_T> vertex);

    /**
     * To which fragment the vertex belongs.
     *
     * @param vertex querying vertex.
     * @return frag id.
     */
    @FFINameAlias("GetFragId")
    int getFragId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetLocalInDegree")
    int getLocalInDegree(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetLocalOutDegree")
    int getLocalOutDegree(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Vertex")
    boolean gid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Vertex2Gid")
    @CXXValue
    VID_T vertex2Gid(@CXXReference Vertex<VID_T> vertex);
}
