/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.example.traverse;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;

public class Traverse
        implements ParallelAppBase<Long, Long, Double, Long, TraverseContext>, ParallelEngine {

    @Override
    public void PEval(
            IFragment<Long, Long, Double, Long> fragment,
            ParallelContextBase<Long, Long, Double, Long> context,
            ParallelMessageManager messageManager) {
        TraverseContext ctx = (TraverseContext) context;
        for (Vertex<Long> vertex : fragment.innerVertices().longIterable()) {
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterable()) {
                Vertex<Long> dst = nbr.neighbor();
                // Update largest distance for current vertex
                ctx.vertexArray.setValue(vertex, Math.max(nbr.data(), ctx.vertexArray.get(vertex)));
            }
        }
        messageManager.forceContinue();
    }

    @Override
    public void IncEval(
            IFragment<Long, Long, Double, Long> fragment,
            ParallelContextBase<Long, Long, Double, Long> context,
            ParallelMessageManager messageManager) {
        TraverseContext ctx = (TraverseContext) context;
        for (Vertex<Long> vertex : fragment.innerVertices().longIterable()) {
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterable()) {
                Vertex<Long> dst = nbr.neighbor();
                // Update largest distance for current vertex
                ctx.vertexArray.setValue(vertex, Math.max(nbr.data(), ctx.vertexArray.get(vertex)));
            }
        }
    }
}
