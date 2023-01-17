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
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead({
    CppHeaderName.PROPERTY_MESSAGE_MANAGER_H,
    CppHeaderName.ARROW_FRAGMENT_H,
    CORE_JAVA_JAVA_MESSAGES_H,
    CppHeaderName.CORE_JAVA_TYPE_ALIAS_H
})
@CXXHead("cstdint")
@FFITypeAlias(CppClassName.PROPERTY_MESSAGE_MANAGER)
public interface PropertyMessageManager extends MessageManagerBase {

    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int eLabelId,
            @CXXReference MSG_T msg);

    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int eLabelId,
            @CXXReference MSG_T msg);

    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int eLabelId,
            @CXXReference MSG_T msg);

    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    @FFINameAlias("GetMessage")
    <FRAG_T, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);
}
