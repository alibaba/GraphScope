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
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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
    @FFINameAlias("InitChannels")
    void initChannels(int channel_num);

    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
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
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>"},
    //            java =
    // {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>"})
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>"},
    //            java =
    // {"com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>"})
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int channel_id);

    @FFINameAlias("GetMessageInBuffer")
    boolean getMessageInBuffer(@CXXReference MessageInBuffer buf);

    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void SendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void SendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            int channel_id);

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
                                synchronized (ParallelMessageManager.class) {
                                    result = getMessageInBuffer(messageInBuffer);
                                }
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

    // default <FRAG_T, MSG_T, MSG_FACTORY_T> void parallelSyncStateOnOuterVertex(FRAG_T frag,
    // VertexRange<Long>
    // vertices, VertexSet vertexSet, int threadNum,
    // ExecutorService executor, Class<MSG_T> msgTClass, Class<MSG_FACTORY_T> msgFactoryTClass) {
    // MSG_FACTORY_T factory = msgFactoryTClass.cast(FFITypeFactory.getFactory(msgTClass));
    //
    //
    // CountDownLatch countDownLatch = new CountDownLatch(threadNum);
    // int originBegin = vertices.begin().GetValue().intValue();
    // int originEnd = vertices.end().GetValue().intValue();
    // AtomicInteger cur = new AtomicInteger(originBegin);
    // int chunkSize = 1024;
    // for (int tid = 0; tid < threadNum; ++tid) {
    // final int finalTid = tid;
    // executor.execute(() -> {
    // Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
    // LongMsg msg = LongMsg.factory.create();
    // int curBegin = 0, curEnd = 0;
    // while (true) {
    // curBegin = Math.min(cur.getAndAdd(chunkSize), originEnd);
    // curEnd = Math.min(curBegin + chunkSize, originEnd);
    // if (curBegin >= curEnd) {
    // break;
    // }
    // for (int i = curBegin; i < curEnd; ++i) {
    // if (vertexSet.get(i)) {
    // vertex.SetValue((long) i);
    // //filler.accept(vertex, msg);
    // syncStateOnOuterVertex(frag, vertex, msg, finalTid);
    // }
    // }
    // }
    // countDownLatch.countDown();
    // });
    // }
    // try {
    // countDownLatch.await();
    // } catch (Exception e) {
    // e.printStackTrace();
    // executor.shutdown();
    // }
    // }
}
