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

package com.alibaba.graphscope.example.projected.sssp;

import com.alibaba.graphscope.app.ProjectedDefaultAppBase;
import com.alibaba.graphscope.context.ProjectedDefaultContextBase;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.example.property.sssp.PropertySSSPVertexDataContext;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A demo implementation of SSSP(Single Source Shortest Path algorithm) on projected graph {@link
 * com.alibaba.graphscope.fragment.ArrowProjectedFragment}, the shared data between supersteps are
 * stored in {@link PropertySSSPVertexDataContext}, which is an implementation of {@link
 * com.alibaba.graphscope.context.VertexDataContext}.
 */
public class ProjectedSSSPVertexData
        implements ProjectedDefaultAppBase<
                Long, Long, Double, Long, ProjectedSSSPVertexDataContext> {
    private static Logger logger = LoggerFactory.getLogger(ProjectedSSSPVertexData.class.getName());

    @Override
    public void PEval(
            ArrowProjectedFragment<Long, Long, Double, Long> fragment,
            ProjectedDefaultContextBase<ArrowProjectedFragment<Long, Long, Double, Long>> context,
            DefaultMessageManager messageManager) {
        ProjectedSSSPVertexDataContext ctx = (ProjectedSSSPVertexDataContext) context;
        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();
        boolean sourceInThisFrag = false;

        sourceInThisFrag = fragment.getInnerVertex(ctx.sourceOid, source);
        logger.info("result " + sourceInThisFrag + ", " + ctx.sourceOid + ", " + source.GetValue());
        if (sourceInThisFrag) {
            ctx.partialResults.setValue(source, 0L);
        } else {
            return;
        }

        ProjectedAdjList<Long, Long> adjList = fragment.getOutgoingAdjList(source);
        long iterTime = 0;
        for (ProjectedNbr<Long, Long> nbr : adjList.iterator()) {
            Vertex<Long> vertex = nbr.neighbor();
            long curDist = nbr.data();
            long prev = ctx.partialResults.get(vertex);
            if (ctx.partialResults.get(vertex) > curDist) {
                ctx.partialResults.setValue(vertex, curDist);
                ctx.nextModified.set(vertex);
            }
            iterTime += 1;
        }

        LongMsg msg = LongMsg.factory.create();
        VertexRange<Long> outerVertices = fragment.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                ctx.nextModified.set(vertex, false);
            }
        }

        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                messageManager.ForceContinue();
                break;
            }
        }
        ctx.curModified.assign(ctx.nextModified);
        ctx.nextModified.clear();
    }

    @Override
    public void IncEval(
            ArrowProjectedFragment<Long, Long, Double, Long> fragment,
            ProjectedDefaultContextBase<ArrowProjectedFragment<Long, Long, Double, Long>> context,
            DefaultMessageManager messageManager) {
        ProjectedSSSPVertexDataContext ctx = (ProjectedSSSPVertexDataContext) context;
        {
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            LongMsg msg = LongMsg.factory.create();
            while (messageManager.getMessage(fragment, vertex, msg)) {
                if (ctx.partialResults.get(vertex) > msg.getData()) {
                    ctx.partialResults.setValue(vertex, msg.getData());
                    ctx.curModified.set(vertex);
                }
            }
        }
        long data_sum = 0;
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (!ctx.curModified.get(vertex)) {
                continue;
            }
            ctx.curModified.set(vertex, false);
            long dist = ctx.partialResults.get(vertex);
            ProjectedAdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            long expectedIter = adjList.size();
            long actualIter = 0;
            for (ProjectedNbr<Long, Long> nbr : adjList.iterator()) {
                Vertex<Long> nbrVertex = nbr.neighbor();
                actualIter += 1;
                long nextDist = dist + nbr.data();
                data_sum += nextDist;
                if (nextDist < ctx.partialResults.get(nbrVertex)) {
                    ctx.partialResults.setValue(nbrVertex, nextDist);
                    ctx.nextModified.set(nbrVertex);
                }
            }
            if (expectedIter != actualIter) {
                logger.info("adjlist iteration false: " + vertex.GetValue());
            }
        }
        logger.info("IncEval " + data_sum);

        // sync out vertices
        LongMsg msg = LongMsg.factory.create();
        VertexRange<Long> outerVertices = fragment.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                ctx.nextModified.set(vertex, false);
            }
        }

        // check condition to move forward
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                messageManager.ForceContinue();
                break;
            }
        }
        ctx.curModified.assign(ctx.nextModified);
        ctx.nextModified.clear();
    }
}
