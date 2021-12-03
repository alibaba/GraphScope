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
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_PARALLEL_MESSAGE_MANAGER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_PARALLEL_MESSAGE_MANAGER_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * The parallel message manager, used in serial apps {@link
 * com.alibaba.graphscope.app.ParallelAppBase}.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@FFITypeAlias(GRAPE_PARALLEL_MESSAGE_MANAGER)
@CXXHead({
    GRAPE_PARALLEL_PARALLEL_MESSAGE_MANAGER_H,
    ARROW_PROJECTED_FRAGMENT_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    GRAPE_ADJ_LIST_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface ParallelMessageManager extends MessageManagerBase {

    default <FRAG_T extends SimpleFragment, MSG_T> boolean syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            syncStateOnOuterVertex((ArrowProjectedFragment) frag, vertex, msg, channelId);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            syncStateOnOuterVertex((ImmutableEdgecutFragment) frag, vertex, msg, channelId);
        }
        return false;
    }

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertexin another
     * fragment.
     *
     * @param frag ImmutableEdgeCutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel id.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertexin another
     * fragment.
     *
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel id.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    /**
     * SyncState on outer vertex without message, used in bfs etc.
     *
     * @param frag fragment.
     * @param vertex query vertex.
     * @param channel_id message channel id.
     * @param <FRAG_T> fragment type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int channel_id);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            sendMsgThroughOEdges((ArrowProjectedFragment) frag, vertex, msg, channelId);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            sendMsgThroughOEdges((ImmutableEdgecutFragment) frag, vertex, msg, channelId);
        }
        return false;
    }

    /**
     * Send the a vertex's data to other fragment througn outgoing edges.
     *
     * @param frag ImmutableEdgeCutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel_id
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    /**
     * Send the a vertex's data to other fragment througn outgoing edges.
     *
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel_id.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            sendMsgThroughEdges((ArrowProjectedFragment) frag, vertex, msg, channelId);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            sendMsgThroughEdges((ImmutableEdgecutFragment) frag, vertex, msg, channelId);
        }
        return false;
    }

    /**
     * Send the a vertex's data to other fragment through incoming and outgoing edges.
     *
     * @param frag ImmutableEdgecutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel_id.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    /**
     * Send the a vertex's data to other fragment through incoming and outgoing edges.
     *
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel_id.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    default <FRAG_T extends SimpleFragment, MSG_T> boolean sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)) {
            sendMsgThroughIEdges((ArrowProjectedFragment) frag, vertex, msg, channelId);
        } else if (frag.fragmentType().equals(ImmutableEdgecutFragmentAdaptor.fragmentType)) {
            sendMsgThroughIEdges((ImmutableEdgecutFragment) frag, vertex, msg, channelId);
        }
        return false;
    }

    /**
     * Send the a vertex's data to other fragment througn incoming edges.
     *
     * @param frag ImmutableEdgecutFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel_id.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    /**
     * Send the a vertex's data to other fragment througn incoming edges.
     *
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     * @param frag ArrowProjectedFragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param channel_id channel_id.
     */
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    /**
     * Init the message manager which number of possible channels. Each channel will swap messages
     * in parallel.
     *
     * @param channel_num number of channels.
     */
    @FFINameAlias("InitChannels")
    void initChannels(int channel_num);

    /**
     * Retrive a message archive.
     *
     * @param buf place to store the archive.
     * @return true if got one.
     */
    @FFINameAlias("GetMessageInBuffer")
    boolean getMessageInBuffer(@CXXReference MessageInBuffer buf);

    /**
     * Parallel processing the messages received from last super step. The user just need to provide
     * a lamba consumer.
     *
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     * @param frag fragment.
     * @param threadNum number of threads to use.
     * @param executor thread pool executor.
     * @param msgSupplier a producer function creating a msg instance.
     * @param consumer lambda function.
     */
    default <FRAG_T, MSG_T> void parallelProcess(
            FRAG_T frag,
            int threadNum,
            ExecutorService executor,
            Supplier<MSG_T> msgSupplier,
            BiConsumer<Vertex<Long>, MSG_T> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
        int chunkSize = 1024;
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            MessageInBuffer messageInBuffer = bufferFactory.create();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            MSG_T msg = msgSupplier.get();
                            boolean result;
                            while (true) {
                                result = getMessageInBuffer(messageInBuffer);
                                if (result) {
                                    while (messageInBuffer.getMessage(frag, vertex, msg)) {
                                        consumer.accept(vertex, msg);
                                    }
                                } else {
                                    break;
                                }
                            }
                            countDownLatch.countDown();
                        }
                    });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            executor.shutdown();
        }
    }
}
