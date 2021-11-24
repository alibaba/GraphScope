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

package com.alibaba.graphscope.example.simple.bfs;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.GrapeAdjList;
import com.alibaba.graphscope.ds.GrapeNbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BFSDefault implements DefaultAppBase<Long, Long, Long, Double, BFSDefaultContext> {
    private static Logger logger = LoggerFactory.getLogger(BFSDefault.class);
    private EmptyType emptyType = EmptyType.factory.create();

    @Override
    public void PEval(
            SimpleFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        BFSDefaultContext ctx = (BFSDefaultContext) context;
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        boolean inThisFrag = fragment.getInnerVertex(ctx.sourceOid, vertex);
        ctx.currentDepth = 1;
        if (inThisFrag) {
            logger.debug("in frag" + fragment.fid() + " " + vertex.GetValue());
            ctx.partialResults.set(vertex, 0);
            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Double> nbr : adjList.iterator()) {
                Vertex<Long> neighbor = nbr.neighbor();
                if (ctx.partialResults.get(neighbor) == Integer.MAX_VALUE) {
                    ctx.partialResults.set(neighbor, 1);
                    if (fragment.isOuterVertex(neighbor)) {
                        messageManager.syncStateOnOuterVertex(fragment, neighbor, emptyType);
                    } else {
                        ctx.currentInnerUpdated.insert(neighbor);
                    }
                }
            }
        }
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            SimpleFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        ImmutableEdgecutFragment<Long, Long, Long, Double> fragment =
                (ImmutableEdgecutFragment<Long, Long, Long, Double>) frag;
        BFSDefaultContext ctx = (BFSDefaultContext) context;
        VertexRange<Long> innerVertices = fragment.innerVertices();
        int nextDepth = ctx.currentDepth + 1;
        ctx.nextInnerUpdated.clear();

        {
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            while (messageManager.getMessage(fragment, vertex, emptyType)) {
                if (ctx.partialResults.get(vertex) == Integer.MAX_VALUE) {
                    ctx.partialResults.set(vertex, ctx.currentDepth);
                    ctx.currentInnerUpdated.set(vertex);
                }
            }
        }
        for (Vertex<Long> cur : innerVertices.locals()) {
            if (ctx.currentInnerUpdated.get(cur)) {
                GrapeAdjList<Long, Double> adjList = fragment.getOutgoingAdjList(cur);
                for (GrapeNbr<Long, Double> nbr : adjList) {
                    Vertex<Long> vertex = nbr.neighbor();
                    if (ctx.partialResults.get(vertex) == Integer.MAX_VALUE) {
                        ctx.partialResults.set(vertex, nextDepth);
                        if (fragment.isOuterVertex(vertex)) {
                            messageManager.syncStateOnOuterVertex(fragment, vertex, emptyType);
                        } else {
                            ctx.nextInnerUpdated.insert(vertex);
                        }
                    }
                }
            }
        }
        ctx.currentDepth = nextDepth;
        if (!ctx.nextInnerUpdated.empty()) {
            messageManager.ForceContinue();
        }
        ctx.currentInnerUpdated.assign(ctx.nextInnerUpdated);
    }
}
