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

package com.alibaba.graphscope.example.property.bfs;

import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.example.property.sssp.ParallelPropertySSSPVertexData;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.google.common.base.Supplier;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertyBfsVertexData
        implements ParallelPropertyAppBase<Long, ParallelPropertyBfsVertexDataContext>,
                ParallelEngine {

    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertySSSPVertexData.class.getName());

    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertyBfsVertexDataContext ctx = (ParallelPropertyBfsVertexDataContext) context;
        messageManager.initChannels(ctx.threadNum);
        ctx.curDepth = 1;
        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();
        Vertex<Long> tmp = FFITypeFactoryhelper.newVertexLong();
        if (fragment.getInnerVertex(0, (long) ctx.sourceOid, source)) {
            ctx.depth.set(source, 0);
            PropertyRawAdjList<Long> adjList = fragment.getOutgoingRawAdjList(source, 0);
            for (PropertyNbrUnit<Long> nbrUnit : adjList.iterator()) {
                long vid = nbrUnit.vid();
                tmp.SetValue(vid);
                if (ctx.depth.get(vid) == Long.MAX_VALUE) {
                    ctx.depth.set(vid, 1);
                    if (fragment.isOuterVertex(tmp)) {
                        messageManager.syncStateOnOuterVertexNoMsg(fragment, tmp, 0, 0L);
                    } else {
                        ctx.curModified.set(vid);
                    }
                }
            }
        }
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {
        ParallelPropertyBfsVertexDataContext ctx = (ParallelPropertyBfsVertexDataContext) context;
        long nextDepth = ctx.curDepth + 1;
        ctx.nextModified.clear();

        Supplier<EmptyType> supplier = () -> EmptyType.factory.create();

        BiConsumer<Vertex<Long>, EmptyType> msgProcessor =
                (vertex, msg) -> {
                    if (ctx.depth.get(vertex) == Long.MAX_VALUE) {
                        ctx.depth.set(vertex, ctx.curDepth);
                        ctx.curModified.set(vertex);
                    }
                };
        messageManager.parallelProcess(
                fragment, ctx.threadNum, ctx.executor, supplier, msgProcessor);

        // iterate and propagate
        BiConsumer<Vertex<Long>, Integer> iterator =
                (vertex, finalTid) -> {
                    PropertyRawAdjList<Long> adjList = fragment.getOutgoingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> nbrUnit : adjList.iterator()) {
                        long vid = nbrUnit.vid();
                        vertex.SetValue(vid);
                        if (ctx.depth.get(vid) == Long.MAX_VALUE) {
                            ctx.depth.set(vid, nextDepth);
                            if (fragment.isOuterVertex(vertex)) {
                                messageManager.syncStateOnOuterVertexNoMsg(
                                        fragment, vertex, finalTid, 2L);
                            } else {
                                ctx.nextModified.set(vid);
                            }
                        }
                    }
                };
        forEachVertex(
                fragment.innerVertices(0), ctx.threadNum, ctx.executor, ctx.curModified, iterator);

        ctx.curDepth = nextDepth;

        if (!ctx.nextModified.empty()) {
            messageManager.ForceContinue();
        }
        ctx.curModified.assign(ctx.nextModified);
    }
}
