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

package com.alibaba.graphscope.column;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GS_VERTEX_ARRAY;
import static com.alibaba.graphscope.utils.CppClassName.INT_COLUMN;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_TYPES_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;

@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@CXXHead(ARROW_FRAGMENT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(GRAPE_TYPES_H)
@FFITypeAlias(INT_COLUMN)
public interface IntColumn<FRAG_T> extends FFIPointer {
    double at(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex);

    void set(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int value);

    @CXXReference
    @FFITypeAlias(GS_VERTEX_ARRAY + "<int32_t>")
    GSVertexArray<Integer> data();
}
