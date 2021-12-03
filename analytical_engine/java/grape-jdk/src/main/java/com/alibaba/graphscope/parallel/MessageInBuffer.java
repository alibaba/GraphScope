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

package com.alibaba.graphscope.parallel;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_MESSAGE_IN_BUFFER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;

@FFIGen(library = JNI_LIBRARY_NAME)
@FFITypeAlias(GRAPE_MESSAGE_IN_BUFFER)
@CXXHead({
    GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    ARROW_PROJECTED_FRAGMENT_H
})
public interface MessageInBuffer extends FFIPointer {
    @FFINameAlias("GetMessage")
    <FRAG_T, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    @FFIFactory
    interface Factory {
        MessageInBuffer create();
    }
}
