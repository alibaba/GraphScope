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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_IMMUTABLE_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.GrapeAdjList;
import com.alibaba.graphscope.ds.Vertex;

/**
 * Java wrapper for grape ImmutableEdgecutFragment.
 *
 * <p>With an edgecut partition, each vertex is assigned to a fragment. In a fragment, inner
 * vertices are those vertices assigned to it, and the outer vertices are the remaining vertices
 * adjacent to some of the inner vertices. The load strategy defines how to store the adjacency
 * between inner and outer vertices.
 *
 * <pre>
 * For example, a graph
 * G = {V, E}
 * V = {v0, v1, v2, v3, v4}
 * E = {(v0, v2), (v0, v3), (v1, v0), (v3, v1), (v3, v4), (v4, v1), (v4, v2)}
 * </pre>
 *
 * <p>Subset V_0 = {v0, v1} is assigned to fragment_0, so InnerVertices_0 = {v0, v1}
 *
 * <p>Subset V_0 = {v0, v1} is assigned to fragment_0, so InnerVertices_0 = {v0, v1}
 *
 * <p>If the load strategy is kOnlyIn: All incoming edges (along with the source vertices) of inner
 * vertices will be stored in a fragment. So,OuterVertices_0 = {v3, v4}, E_0 = {(v1, v0), (v3,
 * v1),(v4, v1)}
 *
 * <p>If the load strategy is kOnlyOut: All outgoing edges (along with the destination vertices) of
 * inner vertices will be stored in a fragment. So, OuterVertices_0 = {v2, v3}, E_0 = {(v0, v2),
 * (v0, v3), (v1, v0)} If the load strategy is kBothOutIn: All incoming edges (along with the source
 * vertices) and outgoing edges (along with destination vertices) of inner vertices will be stored
 * in a fragment. So, OuterVertices_0 = {v2, v3, v4}, E_0 = {(v0, v2), (v0, v3), (v1, v0), (v3, v1),
 * (v4, v1), (v4, v2)}
 *
 * <p>Inner vertices and outer vertices of a fragment will be given a local ID {0, 1, ..., ivnum -
 * 1, ivnum, ..., ivnum + ovnum - 1}, then iterate on vertices can be implemented to increment the
 * local ID. Also, the sets of inner vertices, outer vertices and all vertices are ranges of local
 * ID.
 *
 * @param <OID_T> Type of original vertex ID.
 * @param <VID_T> Type of global vertex ID and local vertex ID.
 * @param <VDATA_T> Type of data on vertices.
 * @param <EDATA_T> Type of data on edges.
 * @see com.alibaba.graphscope.fragment.EdgecutFragment
 * @see com.alibaba.graphscope.fragment.FragmentBase
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H)
@FFITypeAlias(GRAPE_IMMUTABLE_FRAGMENT)
public interface ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue
    GrapeAdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue
    GrapeAdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);
}
