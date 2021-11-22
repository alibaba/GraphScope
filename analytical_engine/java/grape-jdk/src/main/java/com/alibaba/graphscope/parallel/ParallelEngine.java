package com.alibaba.graphscope.parallel;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public interface ParallelEngine {
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
        int chunkSize = 1024;
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
        int chunkSize = 1024;
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
}
