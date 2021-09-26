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

package com.alibaba.graphscope.example.simple.traverse;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.AdjList;
import com.alibaba.graphscope.ds.Nbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPEdata;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPOid;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPVdata;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

public class TraverseMirror
        implements DefaultAppBase<
                SSSPOid, Long, SSSPVdata, SSSPEdata, TraverseMirrorDefaultContext> {
    @Override
    public void PEval(
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> fragment,
            DefaultContextBase<SSSPOid, Long, SSSPVdata, SSSPEdata> defaultContextBase,
            DefaultMessageManager messageManager) {
        TraverseMirrorDefaultContext ctx = (TraverseMirrorDefaultContext) defaultContextBase;
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        // for (Vertex<Long> vertex = innerVertices.begin();
        // vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()){
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            AdjList<Long, SSSPEdata> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, SSSPEdata> cur : adjList) {
                ctx.fake_edata = cur.data().value();
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> fragment,
            DefaultContextBase<SSSPOid, Long, SSSPVdata, SSSPEdata> defaultContextBase,
            DefaultMessageManager messageManager) {
        TraverseMirrorDefaultContext ctx = (TraverseMirrorDefaultContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            AdjList<Long, SSSPEdata> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, SSSPEdata> cur : adjList) {
                ctx.fake_edata = cur.data().value();
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
