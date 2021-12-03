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
package com.alibaba.graphscope.example.property.sssp;

import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.ds.EdgeDataColumn;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.TriConsumer;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertySSSPVertexData
        implements ParallelPropertyAppBase<Long, ParallelPropertySSSPVertexDataContext>,
                ParallelEngine {

    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertySSSPVertexData.class.getName());

    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertySSSPVertexDataContext ctx = (ParallelPropertySSSPVertexDataContext) context;
        messageManager.initChannels(ctx.threadNum);

        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();
        boolean sourceInThisFrag = fragment.getInnerVertex(0, ctx.sourceOid, source);
        if (!sourceInThisFrag) {
            return;
        }

        ctx.partialResults.set(source, 0.0);
        logger.info("source: " + source.GetValue());
        EdgeDataColumn<Long> edgeDataColumn = fragment.edgeDataColumn(0, 0, 2L);

        PropertyRawAdjList<Long> adjList = fragment.getOutgoingRawAdjList(source, 0);
        DoubleMsg msg = DoubleMsg.factory.create();
        Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
        for (PropertyNbrUnit<Long> nbr : adjList.iterator()) {
            //            Vertex<Long> vertex = nbr.getNeighbor();
            long vertexId = nbr.vid();
            cur.SetValue(vertexId);
            ctx.partialResults.set(
                    vertexId, Math.min(ctx.partialResults.get(vertexId), edgeDataColumn.get(nbr)));
            if (fragment.isOuterVertex(cur)) {
                msg.setData(ctx.partialResults.get(vertexId));
                messageManager.syncStateOnOuterVertex(fragment, cur, msg, 0);
            } else {
                ctx.nextModified.set(vertexId);
            }
        }
        messageManager.ForceContinue();
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        // update result
        GSVertexArray<Double> cppPartialResult0 = ctx.cppPartialResult.get(0);
        for (Vertex<Long> vertex : innerVertices.locals()) {
            cppPartialResult0.setValue(vertex, ctx.partialResults.get(0));
        }

        ctx.curModified.assign(ctx.nextModified);
        ctx.nextModified.clear();
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertySSSPVertexDataContext ctx = (ParallelPropertySSSPVertexDataContext) context;
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        EdgeDataColumn<Long> edgeDataColumn = fragment.edgeDataColumn(0, 0, 2L);

        ctx.nextModified.clear();
        // receive msg
        receiveMessage(ctx, fragment, messageManager);

        BiConsumer<Vertex<Long>, Integer> processor =
                (vertex, finalTid) -> {
                    ctx.curModified.set(vertex, false);
                    double dist = ctx.partialResults.get(vertex);
                    PropertyRawAdjList<Long> adjList = fragment.getOutgoingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> nbr : adjList.iterator()) {
                        //                        Vertex<Long> nbrVertex = nbr.getNeighbor();
                        long nbrVertexVid = nbr.vid();
                        double nextDist = dist + edgeDataColumn.get(nbr);
                        if (nextDist < ctx.partialResults.get(nbrVertexVid)) {
                            ctx.partialResults.set(nbrVertexVid, nextDist);
                            ctx.nextModified.set(nbrVertexVid);
                        }
                    }
                };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, ctx.curModified, processor);

        sendMessage(ctx, fragment, messageManager);

        // check condition to move forward
        if (!ctx.nextModified.partialEmpty(0, (int) fragment.getInnerVerticesNum(0))) {
            messageManager.ForceContinue();
        }

        // update result
        GSVertexArray<Double> cppPartialResult0 = ctx.cppPartialResult.get(0);
        for (Vertex<Long> vertex : innerVertices.locals()) {
            cppPartialResult0.setValue(vertex, ctx.partialResults.get(0));
        }

        ctx.curModified.assign(ctx.nextModified);
        ctx.nextModified.clear();
    }

    private void sendMessage(
            ParallelPropertySSSPVertexDataContext context,
            ArrowFragment<Long> frag,
            ParallelPropertyMessageManager messageManager) {
        Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();
        // for outer vertices sync data
        TriConsumer<Vertex<Long>, Integer, DoubleMsg> msgSender =
                (vertex, finalTid, msg) -> {
                    msg.setData(context.partialResults.get(vertex));
                    messageManager.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
                    context.nextModified.set(vertex, false);
                };
        forEachVertexSendMsg(
                frag.outerVertices(0),
                context.threadNum,
                context.executor,
                context.nextModified,
                msgSender,
                msgSupplier);
    }

    private void receiveMessage(
            ParallelPropertySSSPVertexDataContext context,
            ArrowFragment<Long> frag,
            ParallelPropertyMessageManager messageManager) {

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
}
