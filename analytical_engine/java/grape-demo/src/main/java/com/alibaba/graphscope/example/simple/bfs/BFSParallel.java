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

package com.alibaba.graphscope.example.simple.bfs;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BFSParallel
        implements ParallelAppBase<Long, Long, Long, Double, BFSParallelContext>, ParallelEngine {
    private static Logger logger = LoggerFactory.getLogger(BFSParallel.class);

    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> context,
            ParallelMessageManager messageManager) {
        BFSParallelContext ctx = (BFSParallelContext) context;
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        boolean inThisFrag = fragment.getInnerVertex(ctx.sourceOid, vertex);
        messageManager.initChannels(ctx.threadNum);
        ctx.currentDepth = 1;
        if (inThisFrag) {
            logger.info("in frag" + fragment.fid() + " " + vertex.GetValue());
            ctx.partialResults.set(vertex, 0);
            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Double> nbr : adjList.iterator()) {
                Vertex<Long> neighbor = nbr.neighbor();
                if (ctx.partialResults.get(neighbor) == Integer.MAX_VALUE) {
                    ctx.partialResults.set(neighbor, 1);
                    if (fragment.isOuterVertex(neighbor)) {
                        messageManager.syncStateOnOuterVertex(fragment, neighbor, 0);
                    } else {
                        ctx.currentInnerUpdated.set(neighbor);
                    }
                }
            }
        }
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> context,
            ParallelMessageManager messageManager) {
        BFSParallelContext ctx = (BFSParallelContext) context;
        VertexRange<Long> innerVertices = fragment.innerVertices();
        int nextDepth = ctx.currentDepth + 1;
        ctx.nextInnerUpdated.clear();

        BiConsumer<Vertex<Long>, EmptyType> receiveMsg =
                (vertex, msg) -> {
                    if (ctx.partialResults.get(vertex) == Integer.MAX_VALUE) {
                        ctx.partialResults.set(vertex, ctx.currentDepth);
                        ctx.currentInnerUpdated.set(vertex);
                    }
                };
        Supplier<EmptyType> msgSupplier = () -> EmptyType.factory.create();
        messageManager.parallelProcess(
                fragment, ctx.threadNum, ctx.executor, msgSupplier, receiveMsg);

        BiConsumer<Vertex<Long>, Integer> vertexProcessConsumer =
                (cur, finalTid) -> {
                    AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(cur);
                    for (Nbr<Long, Double> nbr : adjList.iterator()) {
                        Vertex<Long> vertex = nbr.neighbor();
                        if (ctx.partialResults.get(vertex) == Integer.MAX_VALUE) {
                            ctx.partialResults.set(vertex, nextDepth);
                            if (fragment.isOuterVertex(vertex)) {
                                messageManager.syncStateOnOuterVertex(fragment, vertex, finalTid);
                            } else {
                                ctx.nextInnerUpdated.insert(vertex);
                            }
                        }
                    }
                };
        forEachVertex(
                innerVertices,
                ctx.threadNum,
                ctx.executor,
                ctx.currentInnerUpdated,
                vertexProcessConsumer);

        ctx.currentDepth = nextDepth;
        if (!ctx.nextInnerUpdated.empty()) {
            messageManager.ForceContinue();
        }
        ctx.currentInnerUpdated.assign(ctx.nextInnerUpdated);
    }
}
