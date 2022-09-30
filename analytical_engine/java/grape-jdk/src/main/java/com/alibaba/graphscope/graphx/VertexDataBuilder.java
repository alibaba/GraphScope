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

package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_DATA_H)
@FFITypeAlias(CppClassName.GS_VERTEX_DATA_BUILDER)
public interface VertexDataBuilder<VID, VD> extends FFIPointer {

    //    @FFINameAlias("Init")
    //    void init(@CXXReference ArrowArrayBuilder<VD> newValues);

    @FFINameAlias("GetArrayBuilder")
    @CXXReference
    VineyardArrayBuilder<VD> getArrayBuilder();

    //    @FFINameAlias("SetBitsetWords")
    //    void setBitsetWords(@CXXReference @FFITypeAlias("arrow::Int64Builder")
    // ArrowArrayBuilder<Long> words);

    @FFINameAlias("MySeal")
    @CXXValue
    StdSharedPtr<VertexData<VID, VD>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<VID, VD> {
        VertexDataBuilder<VID, VD> create(
                @CXXReference VineyardClient client, int fragVnums, VD initValue);

        VertexDataBuilder<VID, VD> create(@CXXReference VineyardClient client, int fragVnums);
    }
}
