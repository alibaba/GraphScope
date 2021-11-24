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

package com.alibaba.graphscope.example.simple.wcc;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class WCCParallel
        implements ParallelAppBase<Long, Long, Long, Double, WCCParallelContext>, ParallelEngine {
    private void PropagateLabelPush(
            SimpleFragment<Long, Long, Long, Double> fragment,
            ParallelContextBase<Long, Long, Long, Double> context,
            ParallelMessageManager mm) {
        WCCParallelContext ctx = (WCCParallelContext) context;
        VertexRange<Long> innerVertices = fragment.innerVertices();
        VertexRange<Long> outerVertices = fragment.outerVertices();
        BiConsumer<Vertex<Long>, Integer> consumer =
                (vertex, finalTid) -> {
                    long cid = ctx.comp_id.get(vertex);
                    // AdjListv2<Long, Double> adjListv2 = frag.GetOutgoingAdjListV2(vertex);
                    AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                    for (Nbr<Long, Double> nbr : adjList.iterator()) {
                        Vertex<Long> cur = nbr.neighbor();
                        if (nbr.neighbor().GetValue().intValue()
                                > fragment.getVerticesNum().intValue()) {
                            System.out.println(
                                    "accessing vertex "
                                            + vertex.GetValue()
                                            + " nbr "
                                            + nbr.neighbor().GetValue().intValue()
                                            + " num vertices "
                                            + fragment.getVerticesNum().intValue());
                            System.out.println("try to get oid" + fragment.getId(nbr.neighbor()));
                        }
                        if (Long.compareUnsigned(ctx.comp_id.get(cur), cid) > 0) {
                            ctx.comp_id.compareAndSetMinUnsigned(cur, cid);
                            ctx.nextModified.set(cur);
                        }
                    }
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, ctx.currModified, consumer);

        BiConsumer<Vertex<Long>, LongMsg> filler =
                (vertex, msg) -> {
                    msg.setData(ctx.comp_id.get(vertex));
                };
        // forEachVertex(outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, consumer2);
        BiConsumer<Vertex<Long>, Integer> msgSender =
                (vertex, finalTid) -> {
                    DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg(ctx.comp_id.get(vertex));
                    mm.syncStateOnOuterVertex(fragment, vertex, msg, finalTid);
                };
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, msgSender);
        // mm.ParallelSyncStateOnOuterVertex(outerVertices, ctx.nextModified, ctx.threadNum,
        // ctx.executor, filler);
    }

    private void PropagateLabelPull(
            SimpleFragment<Long, Long, Long, Double> fragment,
            WCCParallelContext ctx,
            ParallelMessageManager mm) {
        VertexRange<Long> innerVertices = fragment.innerVertices();
        VertexRange<Long> outerVertices = fragment.outerVertices();

        BiConsumer<Vertex<Long>, Integer> outgoing =
                (vertex, finalTid) -> {
                    long oldCid = ctx.comp_id.get(vertex);
                    long newCid = oldCid;
                    AdjList<Long, Double> adjList = fragment.getIncomingAdjList(vertex);
                    for (Nbr<Long, Double> nbr : adjList.iterator()) {
                        long value = ctx.comp_id.get(nbr.neighbor());
                        if (Long.compareUnsigned(value, newCid) < 0) {
                            newCid = value;
                        }
                    }
                    if (Long.compareUnsigned(newCid, oldCid) < 0) {
                        ctx.comp_id.set(vertex, newCid);
                        ctx.nextModified.set(vertex);
                    }
                };
        BiConsumer<Vertex<Long>, Integer> incoming =
                (vertex, finalTid) -> {
                    long oldCid = ctx.comp_id.get(vertex);
                    long newCid = oldCid;
                    AdjList<Long, Double> adjList = fragment.getIncomingAdjList(vertex);
                    for (Nbr<Long, Double> nbr : adjList.iterator()) {
                        long value = ctx.comp_id.get(nbr.neighbor());
                        if (Long.compareUnsigned(value, newCid) < 0) {
                            newCid = value;
                        }
                    }
                    LongMsg msg = LongMsg.factory.create();
                    if (Long.compareUnsigned(newCid, oldCid) < 0) {
                        ctx.comp_id.set(vertex, newCid);
                        ctx.nextModified.set(vertex);
                        msg.setData(newCid);
                        mm.syncStateOnOuterVertex(fragment, vertex, msg, finalTid);
                    }
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, outgoing);
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, incoming);
    }

    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> frag,
            ParallelContextBase<Long, Long, Long, Double> context,
            ParallelMessageManager messageManager) {
        WCCParallelContext ctx = (WCCParallelContext) context;
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        messageManager.initChannels(ctx.threadNum);

        BiConsumer<Vertex<Long>, Integer> consumerInner =
                (vertex, finalTid) -> {
                    ctx.comp_id.set(vertex, frag.getInnerVertexGid(vertex));
                };
        BiConsumer<Vertex<Long>, Integer> consumerOuter =
                (vertex, finalTid) -> {
                    ctx.comp_id.set(vertex, frag.getOuterVertexGid(vertex));
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, consumerInner);
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, consumerOuter);

        PropagateLabelPull(frag, ctx, messageManager);

        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        ctx.currModified.assign(ctx.nextModified);
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> frag,
            ParallelContextBase<Long, Long, Long, Double> context,
            ParallelMessageManager messageManager) {
        ImmutableEdgecutFragment<Long, Long, Long, Double> fragment =
                (ImmutableEdgecutFragment<Long, Long, Long, Double>) frag;
        WCCParallelContext ctx = (WCCParallelContext) context;
        ctx.nextModified.clear();

        BiConsumer<Vertex<Long>, LongMsg> msgReceiveConsumer =
                (vertex, msg) -> {
                    if (Long.compareUnsigned(ctx.comp_id.get(vertex), msg.getData()) > 0) {
                        ctx.comp_id.compareAndSetMinUnsigned(vertex, msg.getData());
                        ctx.currModified.set(vertex);
                    }
                };
        Supplier<LongMsg> msgSupplier = () -> LongMsg.factory.create();
        messageManager.parallelProcess(
                frag, ctx.threadNum, ctx.executor, msgSupplier, msgReceiveConsumer);

        double rate = (double) ctx.currModified.getBitSet().cardinality() / ctx.innerVerticesNum;
        if (rate > 0.1) {
            PropagateLabelPull(frag, ctx, messageManager);
        } else {
            PropagateLabelPush(frag, ctx, messageManager);
        }

        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        ctx.currModified.assign(ctx.nextModified);
    }
}
