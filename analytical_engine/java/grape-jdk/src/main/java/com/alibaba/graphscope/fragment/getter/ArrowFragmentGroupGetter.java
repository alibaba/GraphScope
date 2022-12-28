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

package com.alibaba.graphscope.fragment.getter;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.fragment.ArrowFragmentGroup;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_FRAGMENT_GETTER_H)
@CXXHead(system = {"stdint.h", "memory"})
@CXXHead(CppHeaderName.ARROW_FRAGMENT_GROUP_H)
@CXXHead(CppHeaderName.VINEYARD_CLIENT_H)
@FFITypeAlias(CppClassName.ARROW_FRAGMENT_GROUP_GETTER)
public interface ArrowFragmentGroupGetter extends FFIPointer {

    @FFINameAlias("Get")
    @CXXValue
    @FFITypeAlias("std::shared_ptr<vineyard::ArrowFragmentGroup>")
    StdSharedPtr<ArrowFragmentGroup> get(@CXXReference VineyardClient client, long groupId);

    @FFIFactory
    interface Factory {

        ArrowFragmentGroupGetter create();
    }
}
