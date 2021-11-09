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
import com.alibaba.graphscope.ds.Bitset;
import com.alibaba.graphscope.ds.DenseVertexSet;
import com.alibaba.graphscope.ds.Nbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.DoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

public class SSSPGrapeVertex
        implements DefaultAppBase<Long, Long, Long, Double, SSSPGrapeVertexDefaultContext> {

    @Override
    public void PEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> defaultContextBase,
            DefaultMessageManager mm) {
        SSSPGrapeVertexDefaultContext ctx = (SSSPGrapeVertexDefaultContext) defaultContextBase;

        ctx.execTime -= System.nanoTime();

        DoubleArrayWrapper partialResults = ctx.getPartialResults();
        DenseVertexSet<Long> curModified = ctx.getCurModified();
        DenseVertexSet<Long> nextModified = ctx.getNextModified();

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
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        if (sourceInThisFrag) {
            partialResults.set(source.GetValue(), 0.0);
            AdjList<Long, Double> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, Double> nbr : adjList) {
                Vertex<Long> neigbor = nbr.neighbor();
                partialResults.set(neigbor, Math.min(partialResults.get(neigbor), nbr.data()));
                if (frag.isOuterVertex(neigbor)) {
                    msg.setData(partialResults.get(neigbor));
                    mm.syncStateOnOuterVertex(frag, neigbor, msg);
                } else {
                    nextModified.insert(nbr.neighbor());
                }
            }
        }
        ctx.execTime += System.nanoTime();

        ctx.postProcessTime -= System.nanoTime();
        mm.ForceContinue();
        curModified.swap(nextModified);
        ctx.postProcessTime += System.nanoTime();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        SSSPGrapeVertexDefaultContext ctx = (SSSPGrapeVertexDefaultContext) context;

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
        if (!ctx.nextModified.PartialEmpty(0L, frag.getInnerVerticesNum())) {
            messageManager.ForceContinue();
        }
        // nextModified.swap(curModified);
        ctx.curModified.swap(ctx.nextModified);
        ctx.postProcessTime += System.nanoTime();
    }

    private void receiveMessage(
            SSSPGrapeVertexDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager) {
        ctx.nextModified.clear();
        Vertex<Long> curVertex = frag.innerVertices().begin();
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        try (CXXValueScope scope = new CXXValueScope()) {
            while (messageManager.getMessage(frag, curVertex, msg)) {
                long curLid = curVertex.GetValue();
                if (ctx.partialResults.get(curLid) > msg.getData()) {
                    ctx.partialResults.set(curLid, msg.getData());
                    ctx.curModified.insert(curVertex);
                }
            }
        }
    }

    private void execute(
            SSSPGrapeVertexDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        // BitSet curModifyBS = ctx.curModified.getBitSet();
        Bitset curModifyBS = ctx.curModified.GetBitset();
        VertexRange<Long> innerVertices = frag.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            // int innerVerteicesEnd = innerVertices.end().GetValue().intValue();
            // for (Vertex<Long> vertex = innerVertices.begin();
            // vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()) {
            int vertexLid = vertex.GetValue().intValue();
            if (curModifyBS.getBit(vertexLid)) {
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
                        ctx.nextModified.insert(nbr.neighbor());
                    }
                }
            }
        }
    }

    private void sendMessage(
            SSSPGrapeVertexDefaultContext ctx,
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager) {
        // BitSet nextModifyBS = ctx.nextModified.getBitSet();
        Bitset nextModifyBS = ctx.nextModified.GetBitset();
        VertexRange<Long> outerVertices = frag.outerVertices();
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            // int outerVerticesEnd = outerVertices.end().GetValue().intValue();
            // for (Vertex<Long> vertex = outerVertices.begin();
            // vertex.GetValue().intValue() != outerVerticesEnd; vertex.inc()) {
            if (nextModifyBS.getBit(vertex.GetValue().intValue())) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(frag, vertex, msg);
            }
        }
    }
}
