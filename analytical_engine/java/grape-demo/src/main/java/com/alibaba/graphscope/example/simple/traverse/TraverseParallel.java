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

package com.alibaba.graphscope.example.simple.traverse;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TraverseParallel
        implements ParallelAppBase<Long, Long, Long, Double, TraverseParallelContext>,
                ParallelEngine {
    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> contextBase,
            ParallelMessageManager messageManager) {
        TraverseParallelContext ctx = (TraverseParallelContext) contextBase;
        CountDownLatch latch = new CountDownLatch(ctx.threadNum);
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        AtomicLong cur = new AtomicLong(0L);
        for (int i = 0; i < ctx.threadNum; ++i) {
            ctx.executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            long start = Math.min(cur.getAndAdd(ctx.chunkSize), innerVerteicesEnd);
                            long end = Math.min(start + ctx.chunkSize, innerVerteicesEnd);
                            Vertex<Long> vertex = fragment.innerVertices().begin();
                            for (long lid = start; lid < end; ++lid) {
                                vertex.SetValue(lid);
                                AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                                for (Nbr<Long, Double> cur : adjList.iterator()) {
                                    ctx.fake_edata = cur.data();
                                    ctx.fake_vid = cur.neighbor().GetValue();
                                }
                            }
                            latch.countDown();
                        }
                    });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            ctx.executor.shutdown();
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> contextBase,
            ParallelMessageManager messageManager) {
        TraverseParallelContext ctx = (TraverseParallelContext) contextBase;
        if (ctx.step >= ctx.maxStep) {
            ctx.executor.shutdown();
            return;
        }
        CountDownLatch latch = new CountDownLatch(ctx.threadNum);
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        AtomicLong cur = new AtomicLong(0L);
        for (int i = 0; i < ctx.threadNum; ++i) {
            ctx.executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            long fake_vid = 0L;
                            double fake_edata = 0.0;
                            while (true) {
                                long start =
                                        Math.min(cur.getAndAdd(ctx.chunkSize), innerVerteicesEnd);
                                long end = Math.min(start + ctx.chunkSize, innerVerteicesEnd);
                                Vertex<Long> vertex = fragment.innerVertices().begin();
                                if (start >= innerVerteicesEnd) {
                                    break;
                                }
                                for (long lid = start; lid < end; ++lid) {
                                    vertex.SetValue(lid);
                                    AdjList<Long, Double> adjList =
                                            fragment.getOutgoingAdjList(vertex);
                                    for (Nbr<Long, Double> cur : adjList.iterator()) {
                                        fake_vid = cur.neighbor().GetValue();
                                        fake_edata = cur.data();
                                    }
                                }
                            }
                            ctx.fake_edata = fake_edata;
                            ctx.fake_vid = fake_vid;
                            latch.countDown();
                        }
                    });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            ctx.executor.shutdown();
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
