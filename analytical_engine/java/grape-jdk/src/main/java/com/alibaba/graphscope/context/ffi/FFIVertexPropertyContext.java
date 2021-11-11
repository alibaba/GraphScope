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

import static com.alibaba.graphscope.utils.CppClassName.VERTEX_PROPERTY_CONTEXT;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_TYPES_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.column.DoubleColumn;
import com.alibaba.graphscope.column.IntColumn;
import com.alibaba.graphscope.column.LongColumn;
import com.alibaba.graphscope.context.ContextDataType;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(system = "cstdint")
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(CppHeaderName.VERTEX_PROPERTY_CONTEXT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(GRAPE_TYPES_H)
@FFITypeAlias(VERTEX_PROPERTY_CONTEXT)
public interface FFIVertexPropertyContext<FRAG_T> extends FFIPointer {
    @FFINameAlias("add_column")
    long addColumn(@CXXReference FFIByteString name, @CXXValue ContextDataType contextDataType);

    @FFINameAlias("get_typed_column<double>")
    @CXXValue
    StdSharedPtr<DoubleColumn<FRAG_T>> getDoubleColumn(long index);

    @FFINameAlias("get_typed_column<double>")
    @CXXValue
    StdSharedPtr<DoubleColumn<FRAG_T>> getDoubleColumn(@CXXReference FFIByteString name);

    @FFINameAlias("get_typed_column<uint32_t>")
    @CXXValue
    StdSharedPtr<IntColumn<FRAG_T>> getIntColumn(long index);

    @FFINameAlias("get_typed_column<uint32_t>")
    @CXXValue
    StdSharedPtr<IntColumn<FRAG_T>> getIntColumn(@CXXReference FFIByteString name);

    @FFINameAlias("get_typed_column<uint64_t>")
    @CXXValue
    StdSharedPtr<LongColumn<FRAG_T>> getLongColumn(long index);

    @FFINameAlias("get_typed_column<uint64_t>")
    @CXXValue
    StdSharedPtr<LongColumn<FRAG_T>> getLongColumn(@CXXReference FFIByteString name);

    @FFIFactory
    interface Factory<FRAG_T> {
        FFIVertexPropertyContext<FRAG_T> create(@CXXReference FRAG_T fragment);
    }
}
