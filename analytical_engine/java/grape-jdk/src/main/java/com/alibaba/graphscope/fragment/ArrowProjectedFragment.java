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

import static com.alibaba.graphscope.utils.CppClassName.ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;

/**
 * Java wrapper for <a href=
 * "https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L338">ArrowProjectedFragment</a>
 *
 * @param <OID_T> original id type
 * @param <VID_T> vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(ARROW_PROJECTED_FRAGMENT)
// @CXXTemplate(
// cxx = {"int64_t", "uint64_t", GRAPE_EMPTY_TYPE, "int64_t"},
// java = {"Long", "Long", "com.alibaba.graphscope.ds.EmptyType", "Long"})
// @CXXTemplate(
// cxx = {"int64_t", "uint64_t", "double", "int64_t"},
// java = {"Long", "Long", "Double", "Long"})
public interface ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> extends FFIPointer {
    int fid();

    int fnum();

    @FFINameAlias("Vertices")
    @CXXValue
    VertexRange<VID_T> vertices();

    @FFINameAlias("InnerVertices")
    @CXXValue
    VertexRange<VID_T> innerVertices();

    @FFINameAlias("OuterVertices")
    @CXXValue
    VertexRange<VID_T> outerVertices();

    @FFINameAlias("OuterVertices")
    @CXXValue
    VertexRange<VID_T> outerVertices(int fid);

    @FFINameAlias("GetVertex")
    boolean getVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetId")
    @CXXValue
    OID_T getId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetFragId")
    int getFragId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetData")
    @CXXValue
    VDATA_T getData(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Vertex")
    boolean gid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Vertex2Gid")
    VID_T vertex2Gid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVerticesNum")
    VID_T getInnerVerticesNum();

    @FFINameAlias("GetOuterVerticesNum")
    VID_T getOuterVerticesNum();

    @FFINameAlias("GetVerticesNum")
    VID_T getVerticesNum();

    @FFINameAlias("GetEdgeNum")
    long getEdgeNum();

    @FFINameAlias("GetTotalVerticesNum")
    long getTotalVerticesNum();

    @FFINameAlias("IsInnerVertex")
    boolean isInnerVertex(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("IsOuterVertex")
    boolean isOuterVertex(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVertex")
    boolean getInnerVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOuterVertex")
    boolean getOuterVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVertexId")
    @CXXValue
    OID_T getInnerVertexId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOuterVertexId")
    @CXXValue
    OID_T getOuterVertexId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Oid")
    @CXXValue
    OID_T gid2Oid(VID_T gid);

    @FFINameAlias("Oid2Gid")
    VID_T oid2Gid(@CXXReference OID_T oid);

    @FFINameAlias("InnerVertexGid2Vertex")
    boolean innerVertexGid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("OuterVertexGid2Vertex")
    boolean outerVertexGid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOuterVertexGid")
    VID_T getOuterVertexGid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVertexGid")
    VID_T getInnerVertexGid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue
    ProjectedAdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue
    ProjectedAdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);
}
