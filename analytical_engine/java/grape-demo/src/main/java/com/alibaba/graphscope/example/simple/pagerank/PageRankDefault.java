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

package com.alibaba.graphscope.example.simple.pagerank;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.utils.DoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

public class PageRankDefault extends Communicator
        implements DefaultAppBase<Long, Long, Long, Double, PageRankDefaultContext> {

    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> ctx,
            DefaultMessageManager messageManager) {
        PageRankDefaultContext context = (PageRankDefaultContext) ctx;

        VertexRange<Long> innerVertices = fragment.innerVertices();
        int totalVertexNum = (int) fragment.getTotalVerticesNum();
        context.superStep = 0;
        double base = 1.0 / totalVertexNum;
        double local_dangling_sum = 0.0;

        for (Vertex<Long> vertex : fragment.innerVertices().locals()) {
            AdjList<Long, Double> nbrs = fragment.getOutgoingAdjList(vertex);
            context.degree.set(vertex.GetValue(), (int) nbrs.size());
            DoubleMsg msg = DoubleMsg.factory.create();
            if (nbrs.size() > 0) {
                context.pagerank.set(
                        vertex.GetValue(), base / context.degree.get(vertex.GetValue()));
                msg.setData(context.pagerank.get(vertex.GetValue()));
                messageManager.sendMsgThroughOEdges(fragment, vertex, msg);
            } else {
                context.pagerank.set(vertex.GetValue(), base);
                local_dangling_sum += base;
            }
        }
        DoubleMsg msgDanglingSum = FFITypeFactoryhelper.newDoubleMsg(0.0);
        DoubleMsg localSumMsg = FFITypeFactoryhelper.newDoubleMsg(local_dangling_sum);
        sum(localSumMsg, msgDanglingSum);
        context.danglingSum = msgDanglingSum.getData();

        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> ctx,
            DefaultMessageManager messageManager) {
        PageRankDefaultContext context = (PageRankDefaultContext) ctx;
        int innerVertexNum = fragment.getInnerVerticesNum().intValue();

        context.superStep = context.superStep + 1;
        if (context.superStep > context.maxIteration) {
            for (int i = 0; i < innerVertexNum; ++i) {
                if (context.degree.get(i) != 0) {
                    context.pagerank.set(i, context.degree.get(i) * context.pagerank.get(i));
                }
            }
            return;
        }
        int graphVertexNum = fragment.getVerticesNum().intValue();
        int totalVertexNum = (int) fragment.getTotalVerticesNum();
        double base =
                (1.0 - context.alpha) / totalVertexNum
                        + context.alpha * context.danglingSum / totalVertexNum;

        double local_dangling_sum = 0.0;

        DoubleArrayWrapper nextResult = new DoubleArrayWrapper(innerVertexNum, 0.0);
        // System.out.println("dangling sum: " + context.danglingSum);
        // msgs are all out vertex in this frag, and has incoming edges to the vertex in this frag
        {
            Vertex<Long> vertex = fragment.innerVertices().begin();
            DoubleMsg msg = DoubleMsg.factory.create();
            while (messageManager.getMessage(fragment, vertex, msg)) {
                context.pagerank.set(vertex.GetValue(), msg.getData());
            }
        }

        for (Vertex<Long> vertex : fragment.innerVertices().locals()) {
            if (context.degree.get(vertex.GetValue()) == 0) {
                nextResult.set(vertex, base);
                local_dangling_sum += base;
            } else {
                double cur = 0.0;
                AdjList<Long, Double> nbrs = fragment.getIncomingAdjList(vertex);
                for (Nbr<Long, Double> nbr : nbrs.iterator()) {
                    cur += context.pagerank.get(nbr.neighbor());
                }
                cur = (context.alpha * cur + base) / context.degree.get(vertex.GetValue());
                nextResult.set(vertex.GetValue(), cur);
            }
        }
        DoubleMsg msg = DoubleMsg.factory.create();
        for (Vertex<Long> vertex : fragment.innerVertices().locals()) {
            context.pagerank.set(vertex.GetValue(), nextResult.get(vertex.GetValue()));
            msg.setData(context.pagerank.get(vertex.GetValue()));
            messageManager.sendMsgThroughOEdges(fragment, vertex, msg);
        }

        DoubleMsg msgDanglingSum = FFITypeFactoryhelper.newDoubleMsg(0.0);
        DoubleMsg localSumMsg = FFITypeFactoryhelper.newDoubleMsg(local_dangling_sum);
        sum(localSumMsg, msgDanglingSum);
        context.danglingSum = msgDanglingSum.getData();

        messageManager.ForceContinue();
    }
}
