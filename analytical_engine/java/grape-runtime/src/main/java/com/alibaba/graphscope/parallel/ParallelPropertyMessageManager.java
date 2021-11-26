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
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GS_PARALLEL_PROPERTY_MESSAGE_MANAGER;
import static com.alibaba.graphscope.utils.CppClassName.GS_PRIMITIVE_MESSAGE;
import static com.alibaba.graphscope.utils.CppClassName.LONG_MSG;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_PARALLEL_PARALLEL_PROPERTY_MESSAGE_MANAGER_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.TriConsumer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** As PropertyParalleMessager.h has not much difference from ParallelMessageManager. */
@FFIGen(library = JNI_LIBRARY_NAME)
@FFITypeAlias(GS_PARALLEL_PROPERTY_MESSAGE_MANAGER)
@CXXHead({
    CORE_PARALLEL_PARALLEL_PROPERTY_MESSAGE_MANAGER_H,
    GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H,
    ARROW_FRAGMENT_H,
    CORE_JAVA_TYPE_ALIAS_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface ParallelPropertyMessageManager extends MessageManagerBase {

    @FFINameAlias("InitChannels")
    void initChannels(int channel_num);

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
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ArrowFragment, MSG_T> void syncStateOnOuterVertex(
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
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", "int64_t"},
            java = {"com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>", "Long"})
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ArrowFragment, @FFISkip OID> void syncStateOnOuterVertexNoMsg(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int channel_id,
            @FFISkip OID unused);

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
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ArrowFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int elabelId,
            @CXXReference MSG_T msg,
            int channel_id);

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
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ArrowFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int elabelId,
            @CXXReference MSG_T msg,
            int channel_id);

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
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ArrowFragment, MSG_T> void SendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int elabelId,
            @CXXReference MSG_T msg,
            int channel_id);

    @FFINameAlias("GetMessages")
    boolean getMessageInBuffer(@CXXReference MessageInBuffer buf);

    default <FRAG_T, MSG_T> void parallelProcess(
            FRAG_T frag,
            int vertexLabelId,
            int threadNum,
            ExecutorService executor,
            Supplier<MSG_T> msgSupplier,
            TriConsumer<Vertex<Long>, MSG_T, Integer> consumer) {
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
                                        consumer.accept(vertex, msg, vertexLabelId);
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

    /**
     * This define the parallel process for default labelid, i.e. 0.
     *
     * @param <FRAG_T> fragment type, ArrowFragment
     * @param <MSG_T> message type.
     * @param frag fragment.
     * @param threadNum num thread.
     * @param executor Executor service.
     * @param msgSupplier lambda for msg creation.
     * @param consumer consumer.
     */
    default <FRAG_T, MSG_T> void parallelProcess(
            FRAG_T frag,
            int threadNum,
            ExecutorService executor,
            Supplier<MSG_T> msgSupplier,
            BiConsumer<Vertex<Long>, MSG_T> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
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
                                // not need for synchronization
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
