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
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPEdata;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPOid;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPVdata;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.DoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

public class SSSPMirror
        implements DefaultAppBase<SSSPOid, Long, SSSPVdata, SSSPEdata, SSSPMirrorDefaultContext> {
    @Override
    public void PEval(
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag,
            DefaultContextBase<SSSPOid, Long, SSSPVdata, SSSPEdata> defaultContextBase,
            DefaultMessageManager mm) {
        SSSPMirrorDefaultContext ctx = (SSSPMirrorDefaultContext) defaultContextBase;

        ctx.execTime -= System.nanoTime();

        DoubleArrayWrapper partialResults = ctx.getPartialResults();
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
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        if (sourceInThisFrag) {
            partialResults.set(source.GetValue(), 0.0);
            AdjList<Long, SSSPEdata> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, SSSPEdata> nbr : adjList) {
                Vertex<Long> neigbor = nbr.neighbor();
                partialResults.set(
                        neigbor, Math.min(partialResults.get(neigbor), nbr.data().value()));
                if (frag.isOuterVertex(neigbor)) {
                    msg.setData(partialResults.get(neigbor));
                    mm.syncStateOnOuterVertex(frag, neigbor, msg);
                } else {
                    nextModified.set(neigbor);
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
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag,
            DefaultContextBase<SSSPOid, Long, SSSPVdata, SSSPEdata> context,
            DefaultMessageManager messageManager) {
        SSSPMirrorDefaultContext ctx = (SSSPMirrorDefaultContext) context;

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
            SSSPMirrorDefaultContext ctx,
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag,
            DefaultMessageManager messageManager) {
        ctx.nextModified.clear();
        Vertex<Long> curVertex = frag.innerVertices().begin();
        DoubleMsg msg = DoubleMsg.factory.create();
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
            SSSPMirrorDefaultContext ctx,
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag) {
        // BitSet curModifyBS = ctx.curModified.getBitSet();
        VertexRange<Long> innerVertices = frag.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            int vertexLid = vertex.GetValue().intValue();
            if (ctx.curModified.get(vertexLid)) {
                double curDist = ctx.partialResults.get(vertexLid);
                AdjList<Long, SSSPEdata> adjList = frag.getOutgoingAdjList(vertex);
                for (Nbr<Long, SSSPEdata> nbr : adjList) {
                    long curLid = nbr.neighbor().GetValue();
                    double nextDist = curDist + nbr.data().value();
                    if (nextDist < ctx.partialResults.get(curLid)) {
                        ctx.partialResults.set(curLid, nextDist);
                        ctx.nextModified.set(curLid);
                    }
                }
            }
        }
    }

    private void sendMessage(
            SSSPMirrorDefaultContext ctx,
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag,
            DefaultMessageManager messageManager) {
        // BitSet nextModifyBS = ctx.nextModified.getBitSet();
        VertexRange<Long> outerVertices = frag.outerVertices();
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex.GetValue().intValue())) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(frag, vertex, msg);
            }
        }
    }
}
