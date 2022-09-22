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

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.JNILibraryName;

/**
 * Java wrapper for <a href=
 * "https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L338">ArrowProjectedFragment</a>
 *
 * @param <OID_T> original id type
 * @param <VID_T> vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 */
@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(ARROW_PROJECTED_FRAGMENT)
public interface ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>, FFIPointer {
    long id();

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue
    ProjectedAdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue
    ProjectedAdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("get_out_edges_ptr")
    PropertyNbrUnit<VID_T> getOutEdgesPtr();

    @FFINameAlias("get_in_edges_ptr")
    PropertyNbrUnit<VID_T> getInEdgesPtr();

    @FFINameAlias("get_oe_offsets_begin_ptr")
    long getOEOffsetsBeginPtr();

    @FFINameAlias("get_ie_offsets_begin_ptr")
    long getIEOffsetsBeginPtr();

    @FFINameAlias("get_oe_offsets_end_ptr")
    long getOEOffsetsEndPtr();

    @FFINameAlias("get_ie_offsets_end_ptr")
    long getIEOffsetsEndPtr();

    @FFINameAlias("get_edata_array_accessor")
    @CXXReference
    TypedArray<EDATA_T> getEdataArrayAccessor();

    @FFINameAlias("GetInEdgeNum")
    long getInEdgeNum();

    @FFINameAlias("GetOutEdgeNum")
    long getOutEdgeNum();

    @FFINameAlias("GetData")
    @CXXReference
    VDATA_T getData(@CXXReference Vertex<VID_T> vertex);
}
