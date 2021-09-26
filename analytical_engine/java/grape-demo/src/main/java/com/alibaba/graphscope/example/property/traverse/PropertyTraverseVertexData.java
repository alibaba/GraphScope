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

import com.alibaba.graphscope.app.PropertyDefaultAppBase;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.ds.PropertyAdjList;
import com.alibaba.graphscope.ds.PropertyNbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.PropertyMessageManager;

public class PropertyTraverseVertexData
        implements PropertyDefaultAppBase<Long, PropertyTraverseVertexDataContext> {
    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        PropertyTraverseVertexDataContext ctx = (PropertyTraverseVertexDataContext) context;
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        for (Vertex<Long> vertex : innerVertices.locals()) {
            PropertyAdjList<Long> adjList = fragment.getOutgoingAdjList(vertex, 0);
            for (PropertyNbr<Long> cur : adjList.iterator()) {
                ctx.fake_edata = cur.getDouble(0);
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> fragment,
            PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        PropertyTraverseVertexDataContext ctx = (PropertyTraverseVertexDataContext) context;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        for (Vertex<Long> vertex : innerVertices.locals()) {
            PropertyAdjList<Long> adjList = fragment.getOutgoingAdjList(vertex, 0);
            for (PropertyNbr<Long> cur : adjList.iterator()) {
                ctx.fake_edata = cur.getDouble(0);
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }
        messageManager.ForceContinue();
    }
}
