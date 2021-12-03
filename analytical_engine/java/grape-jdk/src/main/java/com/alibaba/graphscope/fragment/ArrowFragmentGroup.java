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

package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.STD_UNORDERED_MAP;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.StdUnorderedMap;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.ARROW_FRAGMENT_GROUP_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.ARROW_FRAGMENT_GROUP)
public interface ArrowFragmentGroup extends CXXPointer {
    @FFINameAlias("total_frag_num")
    int totalFragNum();

    @FFINameAlias("edge_label_num")
    int edgeLabelNum();

    @FFINameAlias("vertex_label_num")
    int vertexLabelNum();

    // compiling error
    @FFINameAlias("Fragments")
    @CXXReference
    @FFITypeAlias(STD_UNORDERED_MAP + "<unsigned,uint64_t>")
    StdUnorderedMap<Integer, Long> fragments();

    @FFINameAlias("FragmentLocations")
    @CXXReference
    @FFITypeAlias(STD_UNORDERED_MAP + "<unsigned,uint64_t>")
    StdUnorderedMap<Integer, Long> fragmentLocations();
}
