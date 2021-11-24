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

package com.alibaba.graphscope.example.simple.sssp;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.AtomicDoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class SSSPParallel
        implements ParallelAppBase<Long, Long, Long, Double, SSSPParallelContext>, ParallelEngine {
    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> contextBase,
            ParallelMessageManager mm) {
        SSSPParallelContext context = (SSSPParallelContext) contextBase;
        mm.initChannels(context.thread_num());
        context.nextModified.clear();

        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();

        boolean sourceInThisFrag = fragment.getInnerVertex(context.sourceOid, source);
        System.out.println(
                "source in this frag?"
                        + fragment.fid()
                        + ", "
                        + sourceInThisFrag
                        + ", lid: "
                        + source.GetValue());

        AtomicDoubleArrayWrapper partialResults = context.partialResults;
        VertexSet curModified = context.curModified;
        VertexSet nextModified = context.nextModified;
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        if (sourceInThisFrag) {
            partialResults.set(source, 0.0);
            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(source);
            for (Nbr<Long, Double> nbr : adjList.iterator()) {
                Vertex<Long> vertex = nbr.neighbor();
                partialResults.set(vertex, Math.min(nbr.data(), partialResults.get(vertex)));
                if (fragment.isOuterVertex(vertex)) {
                    msg.setData(partialResults.get(vertex));
                    mm.syncStateOnOuterVertex(fragment, vertex, msg, 0);
                } else {
                    nextModified.set(vertex);
                }
            }
        }
        mm.ForceContinue();
        curModified.assign(nextModified);
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> contextBase,
            ParallelMessageManager messageManager) {
        SSSPParallelContext context = (SSSPParallelContext) contextBase;
        context.nextModified.clear();

        // Parallel process the message with the support of JavaMessageInBuffer.
        context.receiveMessageTime -= System.nanoTime();
        receiveMessage(context, fragment, messageManager);
        context.receiveMessageTime += System.nanoTime();

        // Do incremental calculation
        context.execTime -= System.nanoTime();
        execute(context, fragment);
        context.execTime += System.nanoTime();

        context.sendMessageTime -= System.nanoTime();
        sendMessage(context, fragment, messageManager);
        context.sendMessageTime += System.nanoTime();

        if (!context.nextModified.partialEmpty(0, fragment.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        context.curModified.assign(context.nextModified);
    }

    private void receiveMessage(
            SSSPParallelContext context,
            SimpleFragment<Long, Long, Long, Double> frag,
            ParallelMessageManager messageManager) {

        Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();
        BiConsumer<Vertex<Long>, DoubleMsg> messageConsumer =
                (vertex, msg) -> {
                    double preValue = context.partialResults.get(vertex);
                    if (preValue > msg.getData()) {
                        context.partialResults.compareAndSetMin(vertex, msg.getData());
                        context.curModified.set(vertex);
                    }
                };
        messageManager.parallelProcess(
                frag, context.threadNum, context.executor, msgSupplier, messageConsumer);
    }

    private void execute(
            SSSPParallelContext context, SimpleFragment<Long, Long, Long, Double> frag) {
        int innerVerticesNum = frag.getInnerVerticesNum().intValue();

        BiConsumer<Vertex<Long>, Integer> consumer =
                (vertex, finalTid) -> {
                    double curDist = context.partialResults.get(vertex);
                    AdjList<Long, Double> nbrs = frag.getOutgoingAdjList(vertex);
                    for (Nbr<Long, Double> nbr : nbrs.iterator()) {
                        long curLid = nbr.neighbor().GetValue();
                        double nextDist = curDist + nbr.data();
                        if (nextDist < context.partialResults.get(curLid)) {
                            context.partialResults.compareAndSetMin(curLid, nextDist);
                            context.nextModified.set(curLid);
                        }
                    }
                };
        forEachVertex(
                frag.innerVertices(),
                context.threadNum,
                context.executor,
                context.curModified,
                consumer);
    }

    private void sendMessage(
            SSSPParallelContext context,
            SimpleFragment<Long, Long, Long, Double> frag,
            ParallelMessageManager messageManager) {
        // for outer vertices sync data
        BiConsumer<Vertex<Long>, Integer> msgSender =
                (vertex, finalTid) -> {
                    DoubleMsg msg =
                            FFITypeFactoryhelper.newDoubleMsg(context.partialResults.get(vertex));
                    messageManager.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
                };
        forEachVertex(
                frag.outerVertices(),
                context.threadNum,
                context.executor,
                context.nextModified,
                msgSender);
    }
}
