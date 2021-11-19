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

package com.alibaba.graphscope.example.property.traverse;

import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertyTraverseVertexData
        implements ParallelPropertyAppBase<Long, ParallelPropertyTraverseVertexDataContext>,
                ParallelEngine {
    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertyTraverseVertexData.class.getName());

    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {

        ParallelPropertyTraverseVertexDataContext ctx =
                (ParallelPropertyTraverseVertexDataContext) context;
        messageManager.initChannels(ctx.threadNum);

        VertexRange<Long> vertices = fragment.innerVertices(0);

        BiConsumer<Vertex<Long>, Integer> traverser =
                (vertex, finalTid) -> {
                    PropertyRawAdjList<Long> nbrs = fragment.getOutgoingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> nbr : nbrs.iterator()) {
                        ctx.neighboringVertices.set(finalTid, nbr.vid());
                    }
                };
        logger.info("" + ctx.neighboringVertices.get(0));
        forEachVertex(vertices, ctx.threadNum, ctx.executor, traverser);
        ctx.curSteps += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> fragment,
            PropertyParallelContextBase<Long> context,
            ParallelPropertyMessageManager messageManager) {

        ParallelPropertyTraverseVertexDataContext ctx =
                (ParallelPropertyTraverseVertexDataContext) context;
        messageManager.initChannels(ctx.threadNum);

        if (ctx.curSteps >= ctx.maxSteps) {
            return;
        }
        ctx.curSteps += 1;

        VertexRange<Long> vertices = fragment.innerVertices(0);

        BiConsumer<Vertex<Long>, Integer> traverser =
                (vertex, finalTid) -> {
                    PropertyRawAdjList<Long> nbrs = fragment.getOutgoingRawAdjList(vertex, 0);
                    for (PropertyNbrUnit<Long> nbr : nbrs.iterator()) {
                        ctx.neighboringVertices.set(finalTid, nbr.vid());
                    }
                };

        forEachVertex(vertices, ctx.threadNum, ctx.executor, traverser);
        long sum = 0;
        for (int i = 0; i < ctx.neighboringVertices.size(); ++i) {
            sum += ctx.neighboringVertices.get(i);
        }
        logger.info("sum: " + sum);
        messageManager.ForceContinue();
    }
}
