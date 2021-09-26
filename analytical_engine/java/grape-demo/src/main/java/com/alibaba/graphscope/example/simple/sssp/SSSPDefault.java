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

import com.alibaba.fastffi.CXXValueScope;
import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.AdjList;
import com.alibaba.graphscope.ds.Nbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.DoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

public class SSSPDefault implements DefaultAppBase<Long, Long, Long, Double, SSSPDefaultContext> {

    @Override
    public void PEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> ctx,
            DefaultMessageManager mm) {
        SSSPDefaultContext ssspDefaultContext = (SSSPDefaultContext) ctx;

        ssspDefaultContext.execTime -= System.nanoTime();

        DoubleArrayWrapper partialResults = ssspDefaultContext.getPartialResults();
        VertexSet curModified = ssspDefaultContext.getCurModified();
        VertexSet nextModified = ssspDefaultContext.getNextModified();

        nextModified.clear();
        Vertex<Long> source = frag.innerVertices().begin();
        boolean sourceInThisFrag = frag.getInnerVertex(ssspDefaultContext.getSourceOid(), source);
        System.out.println(
                "source in this frag?"
                        + frag.fid()
                        + ", "
                        + sourceInThisFrag
                        + ", lid: "
                        + source.GetValue());
        if (sourceInThisFrag) {
            partialResults.set(source.GetValue(), 0.0);
            AdjList<Long, Double> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, Double> nbr : adjList) {
                Vertex<Long> next = nbr.neighbor();
                partialResults.set(next, Math.min(partialResults.get(next), nbr.data()));
                DoubleMsg msg = DoubleMsg.factory.create();
                if (frag.isOuterVertex(next)) {
                    msg.setData(partialResults.get(next));
                    mm.syncStateOnOuterVertex(frag, next, msg);
                } else {
                    nextModified.set(next);
                }
            }
        }
        ssspDefaultContext.execTime += System.nanoTime();

        ssspDefaultContext.postProcessTime -= System.nanoTime();
        mm.ForceContinue();
        curModified.assign(nextModified);
        ssspDefaultContext.postProcessTime += System.nanoTime();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        SSSPDefaultContext ctx = (SSSPDefaultContext) context;

        ctx.receiveMessageTIme -= System.nanoTime();
        receiveMessage(ctx, frag, messageManager);
        ctx.receiveMessageTIme += System.nanoTime();

        ctx.execTime -= System.nanoTime();
        execute(ctx, frag);
        ctx.execTime += System.nanoTime();

        ctx.sendMessageTime -= System.nanoTime();
        sendMessage(ctx, frag, messageManager);
        ctx.sendMessageTime += System.nanoTime();

        ctx.postProcessTime -= System.nanoTime();
        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        // nextModified.swap(curModified);
        ctx.curModified.assign(ctx.nextModified);
        ctx.postProcessTime += System.nanoTime();
    }

    private void receiveMessage(
            SSSPDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager) {
        ctx.nextModified.clear();
        Vertex<Long> curVertex = FFITypeFactoryhelper.newVertexLong();
        DoubleMsg msg = DoubleMsg.factory.create();
        // double msg = 0.0;
        try (CXXValueScope scope = new CXXValueScope()) {
            while (messageManager.getMessage(frag, curVertex, msg)) {
                long curLid = curVertex.GetValue();
                if (ctx.partialResults.get(curLid) > msg.getData()) {
                    ctx.partialResults.set(curLid, msg.getData());
                    ctx.curModified.set(curLid);
                }
            }
        }
    }

    private void execute(
            SSSPDefaultContext ctx, ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        // BitSet curModifyBS = ctx.curModified.getBitSet();
        VertexRange<Long> innerVertices = frag.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            // int innerVerteicesEnd = innerVertices.end().GetValue().intValue();
            // for (Vertex<Long> vertex = innerVertices.begin();
            // vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()) {
            int vertexLid = vertex.GetValue().intValue();
            if (ctx.curModified.get(vertexLid)) {
                double curDist = ctx.partialResults.get(vertexLid);
                AdjList<Long, Double> adjList = frag.getOutgoingAdjList(vertex);
                // AdjList<Long,Double> adjList = frag.GetOutgoingAdjList(vertex);
                for (Nbr<Long, Double> nbr : adjList) {
                    // long endPointerAddr = adjList.end().getAddress();
                    // long nbrSize = adjList.begin().elementSize();
                    // for (Nbr<Long, Double> nbr = adjList.begin(); nbr.getAddress() !=
                    // endPointerAddr;
                    // nbr.addV(nbrSize)) {
                    long curLid = nbr.neighbor().GetValue();
                    double nextDist = curDist + nbr.data();
                    if (nextDist < ctx.partialResults.get(curLid)) {
                        ctx.partialResults.set(curLid, nextDist);
                        ctx.nextModified.set(curLid);
                    }
                }
            }
        }
    }

    private void sendMessage(
            SSSPDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager) {
        VertexRange<Long> outerVertices = frag.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            DoubleMsg msg = DoubleMsg.factory.create();
            if (ctx.nextModified.get(vertex.GetValue().intValue())) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(frag, vertex, msg);
            }
        }
    }
}
