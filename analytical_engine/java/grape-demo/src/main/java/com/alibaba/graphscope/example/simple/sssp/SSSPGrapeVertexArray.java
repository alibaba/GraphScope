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
import com.alibaba.graphscope.ds.VertexArray;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

public class SSSPGrapeVertexArray
        implements DefaultAppBase<Long, Long, Long, Double, SSSPGrapeVertexDefaultContext> {
    @Override
    public void PEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> defaultContextBase,
            DefaultMessageManager mm) {
        SSSPGrapeVertexArrayDefaultContext ctx =
                (SSSPGrapeVertexArrayDefaultContext) defaultContextBase;

        ctx.execTime -= System.nanoTime();

        VertexArray<Double, Long> partialResults = ctx.getPartialResults();
        VertexSet curModified = ctx.getCurModified();
        VertexSet nextModified = ctx.getNextModified();

        nextModified.clear();
        Vertex<Long> source = frag.innerVertices().begin();
        boolean sourceInThisFrag = frag.getInnerVertex(ctx.getSourceOid(), source);
        System.out.println(
                "source in this frag?"
                        + frag.fid()
                        + ", "
                        + sourceInThisFrag
                        + ", lid: "
                        + source.GetValue());
        if (sourceInThisFrag) {
            partialResults.setValue(source, 0.0);
            AdjList<Long, Double> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, Double> nbr : adjList) {
                Vertex<Long> cur = nbr.neighbor();
                partialResults.setValue(cur, Math.min(partialResults.get(cur), nbr.data()));
                if (frag.isOuterVertex(cur)) {
                    mm.syncStateOnOuterVertex(frag, cur, partialResults.get(cur));
                } else {
                    nextModified.set(cur);
                }
            }
        }
        ctx.execTime += System.nanoTime();

        ctx.postProcessTime -= System.nanoTime();
        mm.ForceContinue();
        curModified.assign(nextModified);
        ctx.postProcessTime += System.nanoTime();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        SSSPGrapeVertexArrayDefaultContext ctx = (SSSPGrapeVertexArrayDefaultContext) context;

        ctx.receiveMessageTime -= System.nanoTime();
        receiveMessage(ctx, frag, messageManager);
        ctx.receiveMessageTime += System.nanoTime();

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
            SSSPGrapeVertexArrayDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager) {
        ctx.nextModified.clear();
        Vertex<Long> curVertex = frag.innerVertices().begin();
        // DoubleMsg msg = DoubleMsg.factory.create();
        double msg = 0.0;
        try (CXXValueScope scope = new CXXValueScope()) {
            while (messageManager.getMessage(frag, curVertex, msg)) {
                long curLid = curVertex.GetValue();
                if (ctx.partialResults.get(curVertex) > msg) {
                    ctx.partialResults.setValue(curVertex, msg);
                    ctx.curModified.set(curLid);
                }
            }
        }
    }

    private void execute(
            SSSPGrapeVertexArrayDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        // BitSet curModifyBS = ctx.curModified.getBitSet();
        // Bitset curModifyBS = ctx.curModified.GetBitset();
        VertexRange<Long> innerVertices = frag.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            // int innerVerteicesEnd = innerVertices.end().GetValue().intValue();
            // for (Vertex<Long> vertex = innerVertices.begin();
            // vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()) {
            int vertexLid = vertex.GetValue().intValue();
            if (ctx.curModified.get(vertexLid)) {
                double curDist = ctx.partialResults.get(vertex);
                AdjList<Long, Double> adjList = frag.getOutgoingAdjList(vertex);
                // AdjList<Long,Double> adjList = frag.GetOutgoingAdjList(vertex);
                for (Nbr<Long, Double> nbr : adjList) {
                    // long endPointerAddr = adjList.end().getAddress();
                    // long nbrSize = adjList.begin().elementSize();
                    // for (Nbr<Long, Double> nbr = adjList.begin(); nbr.getAddress() !=
                    // endPointerAddr;
                    // nbr.addV(nbrSize)) {
                    long curLid = nbr.neighbor().GetValue();
                    Vertex<Long> nbrVertex = nbr.neighbor();
                    double nextDist = curDist + nbr.data();
                    if (nextDist < ctx.partialResults.get(nbrVertex)) {
                        ctx.partialResults.setValue(nbrVertex, nextDist);
                        ctx.nextModified.set(curLid);
                    }
                }
            }
        }
    }

    private void sendMessage(
            SSSPGrapeVertexArrayDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager) {
        // BitSet nextModifyBS = ctx.nextModified.getBitSet();
        // Bitset nextModifyBS = ctx.nextModified.GetBitset();
        VertexRange<Long> outerVertices = frag.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            // int outerVerticesEnd = outerVertices.end().GetValue().intValue();
            // for (Vertex<Long> vertex = outerVertices.begin();
            // vertex.GetValue().intValue() != outerVerticesEnd; vertex.inc()) {
            if (ctx.nextModified.get(vertex.GetValue().intValue())) {
                messageManager.syncStateOnOuterVertex(frag, vertex, ctx.partialResults.get(vertex));
            }
        }
    }
}
