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

package com.alibaba.graphscope.example.property.pagerank;

import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertyPageRankVertexData extends Communicator
        implements ParallelPropertyAppBase<Long, ParallelPropertyPageRankVertexDataContext>,
                ParallelEngine {
    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertyPageRankVertexData.class.getName());

    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertyPageRankVertexDataContext ctx =
                (ParallelPropertyPageRankVertexDataContext) context;
        ctx.superStep = 0;
        messageManager.initChannels(ctx.threadNum);

        int totalVertexNum = (int) fragment.getTotalVerticesNum(0);
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        double base = 1.0 / totalVertexNum;

        TriConsumer<Vertex<Long>, Integer, DoubleMsg> calc =
                (vertex, finalTid, msg) -> {
                    int edgeNum = (int) fragment.getOutgoingAdjList(vertex, 0).size();
                    ctx.degree.set(vertex, edgeNum);
                    if (edgeNum == 0) {
                        ctx.pagerank.setValue(vertex, base);
                    } else {
                        ctx.pagerank.setValue(vertex, base / edgeNum);
                        msg.setData(base / edgeNum);
                        messageManager.sendMsgThroughOEdges(fragment, vertex, 0, msg, finalTid);
                    }
                };
        Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();

        //        ctx.calculationTime -= System.nanoTime();
        forEachVertexSendMsg(innerVertices, ctx.threadNum, ctx.executor, calc, msgSupplier);
        //        ctx.calculationTime += System.nanoTime();

        for (int i = 0; i < fragment.getInnerVerticesNum(0); ++i) {
            if (ctx.degree.get(i) == 0) {
                ctx.danglingVNum += 1;
            }
        }

        DoubleMsg msgDanglingSum = FFITypeFactoryhelper.newDoubleMsg(0.0);
        DoubleMsg localSumMsg = FFITypeFactoryhelper.newDoubleMsg(base * ctx.danglingVNum);
        //        ctx.sumTime -= System.nanoTime();
        sum(localSumMsg, msgDanglingSum);
        //        ctx.sumTime += System.nanoTime();
        ctx.danglingSum = msgDanglingSum.getData();
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertyPageRankVertexDataContext ctx =
                (ParallelPropertyPageRankVertexDataContext) context;
        // int innerVertexNum = fragment.getInnerVerticesNum().intValue();
        //
        ctx.superStep = ctx.superStep + 1;
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        int totalVertexNum = (int) fragment.getTotalVerticesNum(0);

        if (ctx.superStep > ctx.maxIteration) {
            ctx.executor.shutdown();
            return;
        }
        double base =
                (1.0 - ctx.delta) / totalVertexNum + ctx.delta * ctx.danglingSum / totalVertexNum;

        // process received messages
        {
            //            ctx.receiveMsgTime -= System.nanoTime();
            BiConsumer<Vertex<Long>, DoubleMsg> consumer =
                    (vertex, msg) -> {
                        ctx.pagerank.setValue(vertex, msg.getData());
                    };
            Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();
            messageManager.parallelProcess(
                    fragment, ctx.threadNum, ctx.executor, msgSupplier, consumer);
            //            ctx.receiveMsgTime += System.nanoTime();
        } // finish receive data

        // Avoiding creating too much vertex instances in lambda function. We create tmp vertices in
        // advance.
        List<Vertex<Long>> tmpVertices = new ArrayList<>();
        for (int i = 0; i < ctx.threadNum; ++i) {
            tmpVertices.add(FFITypeFactoryhelper.newVertexLong());
        }

        TriConsumer<Vertex<Long>, Integer, DoubleMsg> calcAndSend =
                (vertex, finalTid, msg) -> {
                    if (ctx.degree.get(vertex) == 0) {
                        //                        ctx.nextResult.set(vertex, base);
                        ctx.nextResult.setValue(vertex, base);
                    } else {
                        double cur = 0.0;

                        PropertyRawAdjList<Long> nbrs = fragment.getIncomingRawAdjList(vertex, 0);
                        Vertex<Long> tmpVertex = tmpVertices.get(finalTid);
                        for (PropertyNbrUnit<Long> nbr : nbrs.iterator()) {
                            // Calling nbr.GetNeighbor will cause c++ return a stack object,
                            // resulting creating
                            // create off-heap memery and copy, and creating java objects.
                            tmpVertex.SetValue(nbr.vid());
                            cur += ctx.pagerank.get(tmpVertex);
                        }
                        cur = (cur * ctx.delta + base) / ctx.degree.get(vertex);
                        //                        ctx.nextResult.set(vertex, cur);
                        ctx.nextResult.setValue(vertex, cur);
                        msg.setData(ctx.nextResult.get(vertex));
                        messageManager.sendMsgThroughOEdges(fragment, vertex, 0, msg, finalTid);
                    }
                };
        BiConsumer<Vertex<Long>, Integer> onlyCalc =
                (vertex, finalTid) -> {
                    if (ctx.degree.get(vertex) == 0) {
                        //                        ctx.nextResult.set(vertex, base);
                        ctx.nextResult.setValue(vertex, base);
                    } else {
                        double cur = 0.0;
                        PropertyRawAdjList<Long> nbrs = fragment.getIncomingRawAdjList(vertex, 0);
                        Vertex<Long> tmpVertex = tmpVertices.get(finalTid);
                        for (PropertyNbrUnit<Long> nbr : nbrs.iterator()) {
                            tmpVertex.SetValue(nbr.vid());
                            cur += ctx.pagerank.get(tmpVertex);
                        }
                        cur = (cur * ctx.delta + base) / ctx.degree.get(vertex);
                        //                        ctx.nextResult.set(vertex, cur);
                        ctx.nextResult.setValue(vertex, cur);
                    }
                };
        Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();

        //        ctx.calculationTime -= System.nanoTime();
        if (ctx.superStep != ctx.maxIteration) {
            forEachVertexSendMsg(
                    innerVertices, ctx.threadNum, ctx.executor, calcAndSend, msgSupplier);
        } else {
            forEachVertex(innerVertices, ctx.threadNum, ctx.executor, onlyCalc);
        }

        ctx.pagerank.swap(ctx.nextResult);

        // logger.info("end of sending msg");
        DoubleMsg msgDanglingSum = FFITypeFactoryhelper.newDoubleMsg(0.0);
        DoubleMsg localSumMsg = FFITypeFactoryhelper.newDoubleMsg(base * ctx.danglingVNum);
        //        ctx.sumTime -= System.nanoTime();
        sum(localSumMsg, msgDanglingSum);
        //        ctx.sumTime += System.nanoTime();
        ctx.danglingSum = msgDanglingSum.getData();
        messageManager.ForceContinue();
    }
}
