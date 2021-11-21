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

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.PrimitiveMessage;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.TriConsumer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public interface ParallelEngine {
    int chunkSize = 1024;

    /**
     * Iterate over vertexs in VertexRange, applying lambda functions on each vertex.
     *
     * @param vertices VertexRange obj contains querying vertices
     * @param threadNum number of thread to use
     * @param executor ThreadPoolExecutor to use
     * @param consumer a BiConsumer(lambda function) takes vertex and thread id as input, perform
     *     the desired operation.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default void forEachVertex(
            VertexRange<Long> vertices,
            int threadNum,
            ExecutorService executor,
            BiConsumer<Vertex<Long>, Integer> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                try {
                                    for (int i = curBegin; i < curEnd; ++i) {
                                        vertex.SetValue((long) i);
                                        consumer.accept(vertex, finalTid);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println(
                                            "origin end "
                                                    + originEnd
                                                    + " verteics "
                                                    + curBegin
                                                    + " "
                                                    + curEnd
                                                    + " vertex "
                                                    + vertex.GetValue().intValue()
                                                    + " thread "
                                                    + finalTid);
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
     * Parallel sending messages, with a TriConsumer and msg Supplier
     *
     * @param vertices VertexRange obj contains querying vertices.
     * @param threadNum number of threads to use.
     * @param executor ThreadPoolExecutor to use.
     * @param vertexSet A vertex set, marking querying vertices as true, others false.
     * @param consumer a lambda function representing the per-vertex operation.
     * @param msgSupplier supplier for msg creation.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default <MSG_T extends PrimitiveMessage> void forEachVertex(
            VertexRange<Long> vertices,
            int threadNum,
            ExecutorService executor,
            VertexSet vertexSet,
            TriConsumer<Vertex<Long>, Integer, PrimitiveMessage> consumer,
            Supplier<MSG_T> msgSupplier) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        // int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            PrimitiveMessage msg = msgSupplier.get();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                for (int i = curBegin; i < curEnd; ++i) {
                                    if (vertexSet.get(i)) {
                                        vertex.SetValue((long) i);
                                        consumer.accept(vertex, finalTid, msg);
                                    }
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
     * Apply consumer for each vertex in vertices, in a parallel schema
     *
     * @param vertices VertexRange obj contains querying vertices.
     * @param threadNum number of threads to use.
     * @param executor ThreadPoolExecutor to use.
     * @param vertexSet A vertex set, marking querying vertices as true, others false.
     * @param consumer a lambda function representing the per-vertex operation.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default void forEachVertex(
            VertexRange<Long> vertices,
            int threadNum,
            ExecutorService executor,
            VertexSet vertexSet,
            BiConsumer<Vertex<Long>, Integer> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                for (int i = curBegin; i < curEnd; ++i) {
                                    if (vertexSet.get(i)) {
                                        vertex.SetValue((long) i);
                                        consumer.accept(vertex, finalTid);
                                    }
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
     * Iterate over vertexs in VertexRange, applying lambda functions on each vertex, and send msg
     * with msg created from message supplier
     *
     * @param vertices VertexRange obj contains querying vertices
     * @param threadNum number of thread to use
     * @param executor ThreadPoolExecutor to use
     * @param vertexSet vertexset .
     * @param consumer a BiConsumer(lambda function) takes vertex and thread id as input, perform
     *     the desired operation.
     * @param msgSupplier message creator.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default void forEachVertexSendMsg(
            VertexRange<Long> vertices,
            int threadNum,
            ExecutorService executor,
            VertexSet vertexSet,
            TriConsumer<Vertex<Long>, Integer, DoubleMsg> consumer,
            Supplier<DoubleMsg> msgSupplier) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        // int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            DoubleMsg msg = msgSupplier.get();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                try {
                                    for (int i = curBegin; i < curEnd; ++i) {
                                        if (vertexSet.get(i)) {
                                            vertex.SetValue((long) i);
                                            consumer.accept(vertex, finalTid, msg);
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println(
                                            "origin end "
                                                    + originEnd
                                                    + " verteics "
                                                    + curBegin
                                                    + " "
                                                    + curEnd
                                                    + " vertex "
                                                    + vertex.GetValue().intValue()
                                                    + " thread "
                                                    + finalTid);
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
     * Iterate over vertexs in VertexRange(without vertex set), applying lambda functions on each
     * vertex, and send msg with msg created from message supplier
     *
     * @param vertices VertexRange obj contains querying vertices
     * @param threadNum number of thread to use
     * @param executor ThreadPoolExecutor to use
     * @param consumer a BiConsumer(lambda function) takes vertex and thread id as input, perform
     *     the desired operation.
     * @param msgSupplier message creator.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default void forEachVertexSendMsg(
            VertexRange<Long> vertices,
            int threadNum,
            ExecutorService executor,
            TriConsumer<Vertex<Long>, Integer, DoubleMsg> consumer,
            Supplier<DoubleMsg> msgSupplier) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        // int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            DoubleMsg msg = msgSupplier.get();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                try {
                                    for (int i = curBegin; i < curEnd; ++i) {
                                        vertex.SetValue((long) i);
                                        consumer.accept(vertex, finalTid, msg);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println(
                                            "origin end "
                                                    + originEnd
                                                    + " verteics "
                                                    + curBegin
                                                    + " "
                                                    + curEnd
                                                    + " vertex "
                                                    + vertex.GetValue().intValue()
                                                    + " thread "
                                                    + finalTid);
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
     * Apply Triconsumer for each vertex in vertices, in a parallel schema. Used in property
     * grpah-app where the vertex id label is needed.
     *
     * @param vertices VertexRange obj contains querying vertices.
     * @param vertexLabelId vertex label id.
     * @param threadNum number of threads to use.
     * @param executor ThreadPoolExecutor to use.
     * @param vertexSet A vertex set, marking querying vertices as true, others false.
     * @param consumer a lambda function representing the per-vertex operation.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default void forEachLabelVertex(
            VertexRange<Long> vertices,
            int vertexLabelId,
            int threadNum,
            ExecutorService executor,
            VertexSet vertexSet,
            TriConsumer<Vertex<Long>, Integer, Integer> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        // int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                for (int i = curBegin; i < curEnd; ++i) {
                                    if (vertexSet.get(i)) {
                                        vertex.SetValue((long) i);
                                        consumer.accept(vertex, finalTid, vertexLabelId);
                                    }
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
     * Apply Triconsumer for each vertex in vertices, without checking the vertexSet in a parallel
     * schema. Used in property grpah-app where the vertex id label is needed.
     *
     * @param vertices VertexRange obj contains querying vertices.
     * @param vertexLabelId vertex label id.
     * @param threadNum number of threads to use.
     * @param executor ThreadPoolExecutor to use.
     * @param consumer a lambda function representing the per-vertex operation.
     * @see BiConsumer
     * @see java.util.concurrent.ThreadPoolExecutor
     */
    default void forEachLabelVertex(
            VertexRange<Long> vertices,
            int vertexLabelId,
            int threadNum,
            ExecutorService executor,
            TriConsumer<Vertex<Long>, Integer, Integer> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        // int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Vertex<Long> vertex = vertices.begin();
                            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                            while (true) {
                                int curBegin =
                                        Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                for (int i = curBegin; i < curEnd; ++i) {
                                    vertex.SetValue((long) i);
                                    consumer.accept(vertex, finalTid, vertexLabelId);
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
