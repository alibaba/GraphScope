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

package com.alibaba.graphscope.context.ffi;

import static com.alibaba.graphscope.utils.CppClassName.LABELED_VERTEX_DATA_CONTEXT;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.LABELED_VERTEX_DATA_CONTEXT_H)
@CXXHead(CppHeaderName.ARROW_FRAGMENT_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(LABELED_VERTEX_DATA_CONTEXT)
public interface FFILabeledVertexDataContext<FRAG_T, DATA_T> extends FFIPointer {
    @FFINameAlias("GetValue")
    @CXXValue
    DATA_T getValue(
            @CXXReference @FFITypeAlias(CppClassName.GRAPE_VERTEX + "<uint64_t>")
                    Vertex<Long> vertex);

    @CXXReference
    StdVector<GSVertexArray<DATA_T>> data();

    @FFIFactory
    interface Factory<FRAG_T, DATA_T> {
        FFILabeledVertexDataContext<FRAG_T, DATA_T> create(
                @CXXReference FRAG_T fragment, boolean includeOuter);
    }
}
