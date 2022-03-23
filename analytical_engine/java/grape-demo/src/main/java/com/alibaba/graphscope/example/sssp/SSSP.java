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

package com.alibaba.graphscope.example.sssp;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.utils.AtomicLongArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.Unused;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class SSSP implements ParallelAppBase<Long, Long, Long, Long, SSSPContext>, ParallelEngine {

    private static Logger logger = LoggerFactory.getLogger(SSSP.class);

    @Override
    public void PEval(
            IFragment<Long, Long, Long, Long> fragment,
            ParallelContextBase<Long, Long, Long, Long> contextBase,
            ParallelMessageManager mm) {
        SSSPContext context = (SSSPContext) contextBase;
        mm.initChannels(context.thread_num());
        context.nextModified.clear();

        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();

        boolean sourceInThisFrag = fragment.getInnerVertex(context.sourceOid, source);
        logger.info(
                "source in this frag?"
                        + fragment.fid()
                        + ", "
                        + sourceInThisFrag
                        + ", lid: "
                        + source.GetValue());

        AtomicLongArrayWrapper partialResults = context.partialResults;
        VertexSet curModified = context.curModified;
        VertexSet nextModified = context.nextModified;
        LongMsg msg = FFITypeFactoryhelper.newLongMsg();
        if (sourceInThisFrag) {
            partialResults.set(source, 0);
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(source);
            for (Nbr<Long, Long> nbr : adjList.iterable()) {
                Vertex<Long> vertex = nbr.neighbor();
                partialResults.set(vertex, Math.min(nbr.data(), partialResults.get(vertex)));
                if (fragment.isOuterVertex(vertex)) {
                    msg.setData(partialResults.get(vertex));
                    mm.syncStateOnOuterVertex(
                            fragment,
                            vertex,
                            msg,
                            0,
                            Unused.getUnused(Long.class, Long.class, Long.class));
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
            IFragment<Long, Long, Long, Long> fragment,
            ParallelContextBase<Long, Long, Long, Long> contextBase,
            ParallelMessageManager messageManager) {
        SSSPContext context = (SSSPContext) contextBase;
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

        if (!context.nextModified.partialEmpty(0, (int) fragment.getInnerVerticesNum())) {
            messageManager.ForceContinue();
        }
        context.curModified.assign(context.nextModified);
    }

    private void receiveMessage(
            SSSPContext context,
            IFragment<Long, Long, Long, Long> frag,
            ParallelMessageManager messageManager) {

        Supplier<LongMsg> msgSupplier = () -> LongMsg.factory.create();
        BiConsumer<Vertex<Long>, LongMsg> messageConsumer =
                (vertex, msg) -> {
                    long preValue = context.partialResults.get(vertex);
                    if (preValue > msg.getData()) {
                        context.partialResults.compareAndSetMin(vertex, msg.getData());
                        context.curModified.set(vertex);
                    }
                };
        messageManager.parallelProcess(
                frag,
                context.threadNum,
                context.executor,
                msgSupplier,
                messageConsumer,
                Unused.getUnused(Long.class, Long.class, Long.class));
    }

    private void execute(SSSPContext context, IFragment<Long, Long, Long, Long> frag) {
        int innerVerticesNum = (int) frag.getInnerVerticesNum();

        BiConsumer<Vertex<Long>, Integer> consumer =
                (vertex, finalTid) -> {
                    long curDist = context.partialResults.get(vertex);
                    AdjList<Long, Long> nbrs = frag.getOutgoingAdjList(vertex);
                    for (Nbr<Long, Long> nbr : nbrs.iterable()) {
                        long curLid = nbr.neighbor().GetValue();
                        long nextDist = curDist + nbr.data();
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
            SSSPContext context,
            IFragment<Long, Long, Long, Long> frag,
            ParallelMessageManager messageManager) {
        // for outer vertices sync data
        BiConsumer<Vertex<Long>, Integer> msgSender =
                (vertex, finalTid) -> {
                    LongMsg msg =
                            FFITypeFactoryhelper.newLongMsg(context.partialResults.get(vertex));
                    messageManager.syncStateOnOuterVertex(
                            frag,
                            vertex,
                            msg,
                            finalTid,
                            Unused.getUnused(Long.class, Long.class, Long.class));
                };
        forEachVertex(
                frag.outerVertices(),
                context.threadNum,
                context.executor,
                context.nextModified,
                msgSender);
    }
}
