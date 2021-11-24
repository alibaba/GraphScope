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

package com.alibaba.graphscope.example.property.wcc;

import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.parallel.message.PrimitiveMessage;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.TriConsumer;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertyWCCVertexData
        implements ParallelPropertyAppBase<Long, ParallelPropertyWCCVertexDataContext>,
                ParallelEngine {

    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertyWCCVertexData.class.getName());

    private void PropagateLabelPush(
            ArrowFragment<Long> frag,
            ParallelPropertyWCCVertexDataContext ctx,
            ParallelPropertyMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices(0);
        VertexRange<Long> outerVertices = frag.outerVertices(0);
        BiConsumer<Vertex<Long>, Integer> consumer =
                (vertex, finalTid) -> {
                    long cid = ctx.compId.get(vertex);
                    PropertyRawAdjList<Long> adjList = frag.getOutgoingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> nbr : adjList.iterator()) {
                        long nbrVid = nbr.vid();
                        if (Long.compareUnsigned(ctx.compId.get(nbrVid), cid) > 0) {
                            ctx.compId.compareAndSetMinUnsigned(nbrVid, cid);
                            ctx.nextModified.set(nbrVid);
                        }
                    }
                    adjList = frag.getIncomingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> nbr : adjList.iterator()) {
                        long nbrVid = nbr.vid();
                        if (Long.compareUnsigned(ctx.compId.get(nbrVid), cid) > 0) {
                            ctx.compId.compareAndSetMinUnsigned(nbrVid, cid);
                            ctx.nextModified.set(nbrVid);
                        }
                    }
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, ctx.curModified, consumer);

        // send msg
        TriConsumer<Vertex<Long>, Integer, PrimitiveMessage> msgSend =
                (vertex, finalTid, msg) -> {
                    msg.setData(ctx.compId.get(vertex));
                    mm.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
                };
        Supplier<PrimitiveMessage> msgSupplier =
                () -> FFITypeFactoryhelper.newPrimitiveMsg(Long.class);

        forEachVertex(
                outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, msgSend, msgSupplier);
    }

    @Override
    public void PEval(
            ArrowFragment<Long> frag,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertyWCCVertexDataContext ctx = (ParallelPropertyWCCVertexDataContext) context;
        VertexRange<Long> innerVertices = frag.innerVertices(0);
        VertexRange<Long> outerVertices = frag.outerVertices(0);
        messageManager.initChannels(ctx.threadNum);

        BiConsumer<Vertex<Long>, Integer> consumerInner =
                (vertex, finalTid) -> {
                    ctx.compId.set(vertex, frag.getInnerVertexGid(vertex));
                };
        BiConsumer<Vertex<Long>, Integer> consumerOuter =
                (vertex, finalTid) -> {
                    ctx.compId.set(vertex, frag.getOuterVertexGid(vertex));
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, consumerInner);
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, consumerOuter);

        BiConsumer<Vertex<Long>, Integer> pull =
                (vertex, finalTid) -> {
                    long cid = ctx.compId.get(vertex);
                    PropertyRawAdjList<Long> outEdges = frag.getOutgoingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> edge : outEdges.iterator()) {
                        long vid = edge.vid();
                        if (Long.compareUnsigned(ctx.compId.get(vid), cid) > 0) {
                            ctx.compId.compareAndSetMinUnsigned(vid, cid);
                            ctx.nextModified.set(vid);
                        }
                    }
                    PropertyRawAdjList<Long> inEdges = frag.getIncomingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> edge : inEdges.iterator()) {
                        long vid = edge.vid();
                        if (Long.compareUnsigned(ctx.compId.get(vid), cid) > 0) {
                            ctx.compId.compareAndSetMinUnsigned(vid, cid);
                            ctx.nextModified.set(vid);
                        }
                    }
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, pull);

        // send msg
        TriConsumer<Vertex<Long>, Integer, PrimitiveMessage> msgSend =
                (vertex, finalTid, msg) -> {
                    msg.setData(ctx.compId.get(vertex));
                    messageManager.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
                };
        Supplier<PrimitiveMessage> msgSupplier =
                () -> FFITypeFactoryhelper.newPrimitiveMsg(Long.class);

        forEachVertex(
                outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, msgSend, msgSupplier);

        if (!ctx.nextModified.partialEmpty(0, (int) frag.getInnerVerticesNum(0))) {
            messageManager.ForceContinue();
        }
        ctx.curModified.assign(ctx.nextModified);
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> frag,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertyWCCVertexDataContext ctx = (ParallelPropertyWCCVertexDataContext) context;
        ctx.nextModified.clear();

        BiConsumer<Vertex<Long>, LongMsg> msgReceiveConsumer =
                (vertex, msg) -> {
                    if (Long.compareUnsigned(ctx.compId.get(vertex), msg.getData()) > 0) {
                        ctx.compId.compareAndSetMinUnsigned(vertex, msg.getData());
                        ctx.curModified.set(vertex);
                    }
                };
        Supplier<LongMsg> msgSupplier = () -> LongMsg.factory.create();
        messageManager.parallelProcess(
                frag, ctx.threadNum, ctx.executor, msgSupplier, msgReceiveConsumer);

        PropagateLabelPush(frag, ctx, messageManager);

        if (!ctx.nextModified.partialEmpty(0, (int) frag.getInnerVerticesNum(0))) {
            messageManager.ForceContinue();
        }
        ctx.curModified.assign(ctx.nextModified);
    }
}
