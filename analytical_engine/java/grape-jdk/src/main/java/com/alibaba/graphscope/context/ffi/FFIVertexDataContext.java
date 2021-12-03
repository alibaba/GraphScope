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

import static com.alibaba.graphscope.utils.CppClassName.VERTEX_DATA_CONTEXT;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.VERTEX_DATA_CONTEXT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@FFITypeAlias(VERTEX_DATA_CONTEXT)
public interface FFIVertexDataContext<FRAG_T, DATA_T> extends FFIPointer {

    @CXXReference
    GSVertexArray<DATA_T> data();

    @FFIFactory
    interface Factory<FRAG_T, DATA_T> {
        FFIVertexDataContext<FRAG_T, DATA_T> create(
                @CXXReference FRAG_T fragment, boolean includeOuter);
    }
}
