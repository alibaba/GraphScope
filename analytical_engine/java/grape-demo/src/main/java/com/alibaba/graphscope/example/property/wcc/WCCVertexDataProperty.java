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

import com.alibaba.graphscope.app.DefaultPropertyAppBase;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.PropertyAdjList;
import com.alibaba.graphscope.ds.PropertyNbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.PropertyMessageManager;
import com.alibaba.graphscope.parallel.message.LongMsg;

public class WCCVertexDataProperty
        implements DefaultPropertyAppBase<Long, PropertyWCCVertexDataContext> {
    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        PropertyWCCVertexDataContext ctx = (PropertyWCCVertexDataContext) context;
        int vertexLabelNum = fragment.vertexLabelNum();
        int edgeLabelNum = fragment.edgeLabelNum();
        for (int i = 0; i < vertexLabelNum; ++i) {
            GSVertexArray<Long> curComID = ctx.compId.get(i);
            for (Vertex<Long> vertex : fragment.innerVertices(i).locals()) {
                curComID.setValue(vertex, fragment.getInnerVertexGid(vertex));
            }
            for (Vertex<Long> vertex : fragment.outerVertices(i).locals()) {
                curComID.setValue(vertex, fragment.getOuterVertexGid(vertex));
            }
        }

        //
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            for (Vertex<Long> vertex : innerVertices.locals()) {
                long vertexCompId = ctx.compId.get(i).get(vertex);
                for (int j = 0; j < edgeLabelNum; ++j) {
                    PropertyAdjList<Long> outgoingAdjList = fragment.getOutgoingAdjList(vertex, j);
                    for (PropertyNbr<Long> propertyNbr : outgoingAdjList.iterator()) {
                        Vertex<Long> nbrVertex = propertyNbr.neighbor();
                        int nbrLabel = fragment.vertexLabel(nbrVertex);
                        if (ctx.compId.get(nbrLabel).get(nbrVertex) > vertexCompId) {
                            ctx.compId.get(nbrLabel).setValue(nbrVertex, vertexCompId);
                            ctx.nextModified.get(nbrLabel).set(nbrVertex);
                        }
                    }
                    PropertyAdjList<Long> incomingAdjList = fragment.getIncomingAdjList(vertex, j);
                    for (PropertyNbr<Long> propertyNbr : incomingAdjList.iterator()) {
                        Vertex<Long> nbrVertex = propertyNbr.neighbor();
                        int nbrLabel = fragment.vertexLabel(nbrVertex);
                        if (ctx.compId.get(nbrLabel).get(nbrVertex) > vertexCompId) {
                            ctx.compId.get(nbrLabel).setValue(nbrVertex, vertexCompId);
                            ctx.nextModified.get(nbrLabel).set(nbrVertex);
                        }
                    }
                }
            }
        }
        LongMsg msg = LongMsg.factory.create();
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> outerVertices = fragment.outerVertices(i);
            for (Vertex<Long> vertex : outerVertices.locals()) {
                if (ctx.nextModified.get(i).get(vertex)) {
                    msg.setData(ctx.compId.get(i).get(vertex));
                    messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                    ctx.nextModified.get(i).set(vertex, false);
                }
            }
        }
        for (int i = 0; i < vertexLabelNum; ++i) {
            ctx.curModified.get(i).assign(ctx.nextModified.get(i));
            ctx.nextModified.get(i).clear();
        }
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> graph,
            PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        PropertyWCCVertexDataContext ctx = (PropertyWCCVertexDataContext) context;
    }
}
