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

import static com.alibaba.graphscope.utils.CppClassName.DOUBLE_MSG;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_EMPTY_TYPE;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_MESSAGE_IN_BUFFER;
import static com.alibaba.graphscope.utils.CppClassName.GS_PRIMITIVE_MESSAGE;
import static com.alibaba.graphscope.utils.CppClassName.LONG_MSG;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.CppClassName;

@FFIGen(library = JNI_LIBRARY_NAME)
@FFITypeAlias(GRAPE_MESSAGE_IN_BUFFER)
@CXXHead({
    GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    ARROW_PROJECTED_FRAGMENT_H,
    ARROW_FRAGMENT_H,
    CORE_JAVA_TYPE_ALIAS_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface MessageInBuffer extends FFIPointer {

    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.DoubleMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.LongMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", GS_PRIMITIVE_MESSAGE + "<int64_t>"},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.PrimitiveMessage<java.lang.Long>"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", GRAPE_EMPTY_TYPE},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.ds.EmptyType"
            })
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
