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
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_EDGE_DATA_H)
@CXXHead(CppHeaderName.VINEYARD_ARRAY_BUILDER_H)
@FFITypeAlias(CppClassName.GS_EDGE_DATA_BUILDER)
public interface EdgeDataBuilder<VID, ED> extends FFIPointer {

    @FFINameAlias("GetArrayBuilder")
    @CXXReference
    VineyardArrayBuilder<ED> getArrayBuilder();

    @FFINameAlias("MySeal")
    @CXXValue
    StdSharedPtr<EdgeData<VID, ED>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<VID, ED> {

        EdgeDataBuilder<VID, ED> create(
                @CXXReference VineyardClient client, @CXXReference StdVector<ED> arrayBuilder);

        EdgeDataBuilder<VID, ED> create(@CXXReference VineyardClient client, long size);

        default EdgeData<VID, ED> createAndBuild(VineyardClient client, StdVector<ED> newValues) {
            EdgeDataBuilder<VID, ED> builder = create(client, newValues);
            StdSharedPtr<EdgeData<VID, ED>> res = builder.seal(client);
            return res.get();
        }
    }
}
