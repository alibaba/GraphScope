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

import static com.alibaba.graphscope.parallel.MessageUtils.getUnused;
import static com.alibaba.graphscope.parallel.MessageUtils.getUnusedNoMsg;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_PARALLEL_MESSAGE_MANAGER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_PARALLEL_MESSAGE_MANAGER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * The parallel message manager, used in serial apps {@link com.alibaba.graphscope.app.ParallelAppBase}.
 */
@FFIGen
@FFITypeAlias(GRAPE_PARALLEL_MESSAGE_MANAGER)
@CXXHead({
    GRAPE_PARALLEL_PARALLEL_MESSAGE_MANAGER_H,
    ARROW_PROJECTED_FRAGMENT_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    GRAPE_ADJ_LIST_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface ParallelMessageManager extends MessageManagerBase {

    default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> boolean syncStateOnOuterVertex(
            @CXXReference IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> arrowProjectedAdaptor =
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag);
            syncStateOnOuterVertexArrowProjected(
                    arrowProjectedAdaptor.getArrowProjectedFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(arrowProjectedAdaptor, msg.getClass()));
        } else if (frag.fragmentType().equals(FragmentType.ImmutableEdgecutFragment)) {
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor =
                    (ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag;
            syncStateOnOuterVertexImmutable(
                    immutableAdaptor.getImmutableFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(immutableAdaptor, msg.getClass()));
        } else {
            throw new IllegalStateException("Not supported now");
        }
        return false;
    }

    default <OID_T, VID_T, VDATA_T, EDATA_T> boolean syncStateOnOuterVertexNoMsg(
            @CXXReference IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            int channelId) {
        if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> arrowProjectedAdaptor =
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag);
            syncStateOnOuterVertexArrowProjectedNoMsg(
                    arrowProjectedAdaptor.getArrowProjectedFragment(),
                    vertex,
                    channelId,
                    getUnusedNoMsg(arrowProjectedAdaptor));
        } else if (frag.fragmentType().equals(FragmentType.ImmutableEdgecutFragment)) {
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor =
                    (ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag;
            syncStateOnOuterVertexImmutableNoMsg(
                    immutableAdaptor.getImmutableFragment(),
                    vertex,
                    channelId,
                    getUnusedNoMsg(immutableAdaptor));
        } else {
            throw new IllegalStateException("Not supported now");
        }
        return false;
    }

    default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> boolean sendMsgThroughOEdges(
            @CXXReference IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> arrowProjectedAdaptor =
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag);
            sendMsgThroughOEdgesArrowProjected(
                    arrowProjectedAdaptor.getArrowProjectedFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(arrowProjectedAdaptor, msg.getClass()));
        } else if (frag.fragmentType().equals(FragmentType.ImmutableEdgecutFragment)) {
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor =
                    (ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag;
            sendMsgThroughOEdgesImmutable(
                    immutableAdaptor.getImmutableFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(immutableAdaptor, msg.getClass()));
        } else {
            throw new IllegalStateException("Not supported now");
        }
        return false;
    }

    default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> boolean sendMsgThroughEdges(
            @CXXReference IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> arrowProjectedAdaptor =
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag);
            sendMsgThroughEdgesArrowProjected(
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag)
                            .getArrowProjectedFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(arrowProjectedAdaptor, msg.getClass()));
        } else if (frag.fragmentType().equals(FragmentType.ImmutableEdgecutFragment)) {
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor =
                    (ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag;
            sendMsgThroughEdgesImmutable(
                    immutableAdaptor.getImmutableFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(immutableAdaptor, msg.getClass()));
        } else {
            throw new IllegalStateException("Not supported now");
        }
        return false;
    }

    default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> boolean sendMsgThroughIEdges(
            @CXXReference IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            @CXXReference MSG_T msg,
            int channelId) {
        if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> arrowProjectedAdaptor =
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag);
            sendMsgThroughIEdgesArrowProjected(
                    arrowProjectedAdaptor.getArrowProjectedFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(arrowProjectedAdaptor, msg.getClass()));
        } else if (frag.fragmentType().equals(FragmentType.ImmutableEdgecutFragment)) {
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor =
                    (ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag;
            sendMsgThroughIEdgesImmutable(
                    immutableAdaptor.getImmutableFragment(),
                    vertex,
                    msg,
                    channelId,
                    getUnused(immutableAdaptor, msg.getClass()));
        } else {
            throw new IllegalStateException("Not supported now");
        }
        return false;
    }

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

    @FFINameAlias("SendToFragment")
    <MSG_T> void sendToFragment(int dstFid, @CXXReference MSG_T msg, int channelId);

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertexin another
     * fragment.
     *
     * @param frag       ImmutableEdgeCutFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel id.
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void syncStateOnOuterVertexImmutable(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertexin another
     * fragment.
     *
     * @param frag       ArrowProjectedFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel id.
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void syncStateOnOuterVertexArrowProjected(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * SyncState on outer vertex without message, used in bfs etc.
     *
     * @param frag       fragment.
     * @param vertex     query vertex.
     * @param channel_id message channel id.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    @FFISkip UNUSED_T>
            void syncStateOnOuterVertexImmutableNoMsg(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    int channel_id,
                    @FFISkip UNUSED_T vdata);

    /**
     * SyncState on outer vertex without message, used in bfs etc.
     *
     * @param frag       fragment.
     * @param vertex     query vertex.
     * @param channel_id message channel id.
     */
    @FFINameAlias("SyncStateOnOuterVertex")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    @FFISkip UNUSED_T>
            void syncStateOnOuterVertexArrowProjectedNoMsg(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    int channel_id,
                    @FFISkip UNUSED_T vdata);

    /**
     * Send the a vertex's data to other fragment througn outgoing edges.
     *
     * @param frag       ImmutableEdgeCutFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel_id
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SendMsgThroughOEdges")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void sendMsgThroughOEdgesImmutable(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Send the a vertex's data to other fragment througn outgoing edges.
     *
     * @param frag       ArrowProjectedFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel_id.
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SendMsgThroughOEdges")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void sendMsgThroughOEdgesArrowProjected(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Send the a vertex's data to other fragment through incoming and outgoing edges.
     *
     * @param frag       ImmutableEdgecutFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel_id.
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SendMsgThroughEdges")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void sendMsgThroughEdgesImmutable(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Send the a vertex's data to other fragment through incoming and outgoing edges.
     *
     * @param frag       ArrowProjectedFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel_id.
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SendMsgThroughEdges")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void sendMsgThroughEdgesArrowProjected(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Send the a vertex's data to other fragment througn incoming edges.
     *
     * @param frag       ImmutableEdgecutFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel_id.
     * @param <MSG_T>    message type.
     */
    @FFINameAlias("SendMsgThroughIEdges")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void sendMsgThroughIEdgesImmutable(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Send the a vertex's data to other fragment througn incoming edges.
     *
     * @param <MSG_T>    message type.
     * @param frag       ArrowProjectedFragment.
     * @param vertex     querying vertex.
     * @param msg        msg to send.
     * @param channel_id channel_id.
     */
    @FFINameAlias("SendMsgThroughIEdges")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            void sendMsgThroughIEdgesArrowProjected(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    int channel_id,
                    @FFISkip UNUSED_T unused);

    /**
     * Parallel processing the messages received from last super step. The user just need to provide
     * a lamba consumer.
     *
     * @param <MSG_T>     message type.
     * @param frag        fragment.
     * @param threadNum   number of threads to use.
     * @param executor    thread pool executor.
     * @param msgSupplier a producer function creating a msg instance.
     * @param consumer    lambda function.
     */
    default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> void parallelProcess(
            IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            int threadNum,
            ExecutorService executor,
            Supplier<MSG_T> msgSupplier,
            BiConsumer<Vertex<VID_T>, MSG_T> consumer) {
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
                            Vertex<VID_T> vertex =
                                    FFITypeFactoryhelper.newVertex(frag.getVidClass());
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
