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

package com.alibaba.graphscope.fragment.mapper;

import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_MAPPER_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_MAPPER_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CPP_ARROW_PROJECTED_FRAGMENT_MAPPER)
public interface ArrowProjectedFragmentMapper<OID_T, VID_T, NEW_V_T, NEW_E_T> extends FFIPointer {

    @FFINameAlias("Map")
    @CXXValue
    StdSharedPtr<ArrowProjectedFragment<OID_T, VID_T, NEW_V_T, NEW_E_T>> map(
            @CXXReference StdSharedPtr<ArrowFragment<OID_T>> oldFrag,
            int vLabelId,
            int eLabel,
            @CXXReference ArrowArrayBuilder<NEW_V_T> vdBuilder,
            @CXXReference ArrowArrayBuilder<NEW_E_T> edBuilder,
            @CXXReference VineyardClient client);

    // only vd
    @FFINameAlias("Map")
    @CXXValue
    StdSharedPtr<ArrowProjectedFragment<OID_T, VID_T, NEW_V_T, NEW_E_T>> map(
            @CXXReference StdSharedPtr<ArrowFragment<OID_T>> oldFrag,
            int vLabe,
            int oldEPropId,
            @CXXReference ArrowArrayBuilder<NEW_V_T> vdBuilder,
            @CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T, VID_T, NEW_V_T, NEW_E_T> {

        ArrowProjectedFragmentMapper<OID_T, VID_T, NEW_V_T, NEW_E_T> create();
    }
}
