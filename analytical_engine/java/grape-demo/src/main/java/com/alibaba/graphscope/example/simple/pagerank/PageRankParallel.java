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

package com.alibaba.graphscope.example.simple.pagerank;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankParallel extends Communicator
        implements ParallelAppBase<Long, Long, Long, Double, PageRankParallelContext>,
                ParallelEngine {
    private static Logger logger = LoggerFactory.getLogger(PageRankParallel.class);

    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> contextBase,
            ParallelMessageManager javaParallelMessageManager) {
        PageRankParallelContext ctx = (PageRankParallelContext) contextBase;
        javaParallelMessageManager.initChannels(ctx.thread_num());
        VertexRange<Long> innerVertices = fragment.innerVertices();
        int totalVertexNum = (int) fragment.getTotalVerticesNum();

        ctx.superStep = 0;
        double base = 1.0 / totalVertexNum;

        BiConsumer<Vertex<Long>, Integer> calc =
                (Vertex<Long> vertex, Integer finalTid) -> {
                    int edgeNum = (int) fragment.getOutgoingAdjList(vertex).size();
                    ctx.degree.set(vertex, edgeNum);
                    if (edgeNum == 0) {
                        ctx.pagerank.set(vertex, base);
                    } else {
                        ctx.pagerank.set(vertex, base / edgeNum);
                        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg(base / edgeNum);
                        javaParallelMessageManager.sendMsgThroughOEdges(
                                fragment, vertex, msg, finalTid);
                    }
                };
        forEachVertex(innerVertices, ctx.thread_num, ctx.executor, calc);
        int innerVertexSize = fragment.getInnerVerticesNum().intValue();
        for (int i = 0; i < innerVertexSize; ++i) {
            if (ctx.degree.get(i) == 0) {
                ctx.danglingVNum += 1;
            }
        }
        DoubleMsg msgDanglingSum = FFITypeFactoryhelper.newDoubleMsg(0.0);
        DoubleMsg localSumMsg = FFITypeFactoryhelper.newDoubleMsg(base * ctx.danglingVNum);
        sum(localSumMsg, msgDanglingSum);
        ctx.danglingSum = msgDanglingSum.getData();
        javaParallelMessageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> contextBase,
            ParallelMessageManager javaParallelMessageManager) {

        PageRankParallelContext ctx = (PageRankParallelContext) contextBase;
        int innerVertexNum = fragment.getInnerVerticesNum().intValue();
        VertexRange<Long> innerVertices = fragment.innerVertices();

        ctx.superStep = ctx.superStep + 1;
        if (ctx.superStep > ctx.maxIteration) {
            for (int i = 0; i < innerVertexNum; ++i) {
                if (ctx.degree.get(i) != 0) {
                    ctx.pagerank.set(i, ctx.degree.get(i) * ctx.pagerank.get(i));
                }
            }
            ctx.executor.shutdown();
            return;
        }

        int totalVertexNum = (int) fragment.getTotalVerticesNum();
        double base =
                (1.0 - ctx.alpha) / totalVertexNum + ctx.alpha * ctx.danglingSum / totalVertexNum;

        // process received messages
        {
            BiConsumer<Vertex<Long>, DoubleMsg> consumer =
                    ((vertex, aDouble) -> {
                        ctx.pagerank.set(vertex, aDouble.getData());
                    });
            Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();
            javaParallelMessageManager.parallelProcess(
                    fragment, ctx.thread_num, ctx.executor, msgSupplier, consumer);
        } // finish receive data
        // logger.info("end of receiving data");

        BiConsumer<Vertex<Long>, Integer> calc =
                ((vertex, finalTid) -> {
                    if (ctx.degree.get(vertex) == 0) {
                        ctx.nextResult.set(vertex, base);
                    } else {
                        double cur = 0.0;
                        AdjList<Long, Double> nbrs = fragment.getIncomingAdjList(vertex);
                        for (Nbr<Long, Double> nbr : nbrs.iterator()) {
                            cur += ctx.pagerank.get(nbr.neighbor());
                        }
                        cur = (cur * ctx.alpha + base) / ctx.degree.get(vertex);
                        ctx.nextResult.set(vertex, cur);
                        DoubleMsg msg =
                                FFITypeFactoryhelper.newDoubleMsg(ctx.nextResult.get(vertex));
                        javaParallelMessageManager.sendMsgThroughOEdges(
                                fragment, vertex, msg, finalTid);
                    }
                });
        forEachVertex(innerVertices, ctx.thread_num, ctx.executor, calc);

        {
            double timeSwapStart = System.nanoTime();
            BiConsumer<Vertex<Long>, Integer> consumer =
                    ((vertex, integer) -> {
                        ctx.pagerank.set(vertex, ctx.nextResult.get(vertex));
                    });
            forEachVertex(innerVertices, ctx.thread_num, ctx.executor, consumer);
            ctx.swapTime += (System.nanoTime() - timeSwapStart);
        }

        // logger.info("end of sending msg");
        double time0 = System.nanoTime();
        DoubleMsg msgDanglingSum = FFITypeFactoryhelper.newDoubleMsg(0.0);
        DoubleMsg localSumMsg = FFITypeFactoryhelper.newDoubleMsg(base * ctx.danglingVNum);
        sum(localSumMsg, msgDanglingSum);
        ctx.danglingSum = msgDanglingSum.getData();
        ctx.sumDoubleTime += System.nanoTime() - time0;
        javaParallelMessageManager.ForceContinue();
    }
}
