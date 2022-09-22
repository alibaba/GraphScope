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

package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GS_DEFAULT_IMMUTABLE_CSR;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_DEFAULT_IMMUTABLE_CSR)
public interface ImmutableCSR<VID_T, ED> extends FFIPointer { // grape::Nbr

    @FFINameAlias("vertex_num")
    VID_T vertexNum();

    @FFINameAlias("edge_num")
    long edgeNum();

    @FFINameAlias("degree")
    int degree(VID_T lid);

    @FFINameAlias("is_empty")
    boolean isEmpty(VID_T lid);

    @FFINameAlias("get_begin")
    GrapeNbr<VID_T, ED> getBegin(VID_T lid);

    @FFINameAlias("get_end")
    GrapeNbr<VID_T, ED> getEnd(VID_T lid);
}
