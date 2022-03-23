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

package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.GRAPHX_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(GRAPHX_FRAGMENT)
public interface GraphXStringEDFragment<OID_T, VID_T, VD_T, ED_T>
        extends BaseGraphXFragment<OID_T, VID_T, VD_T, ED_T> {

    @FFINameAlias("GetEdataArray")
    @CXXReference
    StringTypedArray getEdataArray();

    @FFINameAlias("GetVdataArray")
    @CXXReference
    TypedArray<VD_T> getVdataArray();

    @FFINameAlias("GetData")
    @CXXReference
    VD_T getData(@CXXReference Vertex<VID_T> vertex);
}
