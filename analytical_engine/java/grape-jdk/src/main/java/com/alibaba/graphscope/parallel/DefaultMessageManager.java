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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_DEFAULT_MESSAGE_MANAGER;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_DEFAULT_MESSAGE_MANAGER_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;

/**
 * The default message manager, used in serial apps {@link DefaultAppBase} and {@link
 * DefaultAppBase}.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@FFITypeAlias(GRAPE_DEFAULT_MESSAGE_MANAGER)
@CXXHead({
    GRAPE_ADJ_LIST_H,
    GRAPE_PARALLEL_DEFAULT_MESSAGE_MANAGER_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    ARROW_PROJECTED_FRAGMENT_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface DefaultMessageManager extends MessageManagerBase {

    default <FRAG_T extends SimpleFragment, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            getMessage((ArrowProjectedFragment) frag, vertex, msg);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            getMessage((ImmutableEdgecutFragment) frag, vertex, msg);
        }
        return false;
    }

    /**
     * Get the message received for specified vertex during last super step.
     *
     * @param frag bound fragment.
     * @param vertex querying vertex.
     * @param msg msg place hold.
     * @param <FRAG_T> frag type.
     * @param <MSG_T> msg type.
     * @return true if really got a message.
     */
    @FFINameAlias("GetMessage")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Get the message received for specified vertex during last super step.
     *
     * @param frag bound fragment.
     * @param vertex querying vertex.
     * @param msg msg place hold.
     * @param <FRAG_T> frag type.
     * @param <MSG_T> msg type.
     * @return true if really got a message.
     */
    @FFINameAlias("GetMessage")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            syncStateOnOuterVertex((ArrowProjectedFragment) frag, vertex, msg);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            syncStateOnOuterVertex((ImmutableEdgecutFragment) frag, vertex, msg);
        }
        return false;
    }

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertex.
     *
     * @param frag fragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertex.
     *
     * @param frag fragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            sendMsgThroughOEdges((ArrowProjectedFragment) frag, vertex, msg);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            sendMsgThroughOEdges((ImmutableEdgecutFragment) frag, vertex, msg);
        }
        return false;
    }

    /**
     * Send the a vertex's data to other fragment througn outgoing edges.
     *
     * @param frag ImmutableEdgeCutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send the a vertex's data to other fragment throughn outgoing edges.
     *
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            sendMsgThroughEdges((ArrowProjectedFragment) frag, vertex, msg);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            sendMsgThroughEdges((ImmutableEdgecutFragment) frag, vertex, msg);
        }
        return false;
    }

    /**
     * Send the a vertex's data to other fragment throughn incoming and outgoing edges.
     *
     * @param frag ImmutableEdgeCutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send the a vertex's data to other fragment throughn incoming and outgoing edges.
     *
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            sendMsgThroughIEdges((ArrowProjectedFragment) frag, vertex, msg);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            sendMsgThroughIEdges((ImmutableEdgecutFragment) frag, vertex, msg);
        }
        return false;
    }

    /**
     * Send the a vertex's data to other fragment throughn incoming edges.
     *
     * @param frag ImmutableEdgecutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send the a vertex's data to other fragment throughn incoming edges.
     *
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);
}
