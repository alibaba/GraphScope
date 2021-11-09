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
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

public class TraverseWhile
        implements DefaultAppBase<Long, Long, Long, Double, TraverseWhileContext> {
    @Override
    public void PEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> defaultContextBase,
            DefaultMessageManager messageManager) {
        TraverseWhileContext ctx = (TraverseWhileContext) defaultContextBase;
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        // for (Vertex<Long> vertex = innerVertices.begin();
        // vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()){
        for (int i = 0; i < innerVerteicesEnd; ++i) {
            vertex.SetValue((long) i);
            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
            Nbr<Long, Double> cur = adjList.begin();
            // Nbr<Long, Double> cur = fragment.GetOutgoingAdjListBegin(i);
            long elementSize = cur.elementSize();
            long endPointerAddr = adjList.end().getAddress();
            // long endPointerAddr = fragment.GetOutgoingAdjListEnd(i).getAddress();
            while (cur.getAddress() != endPointerAddr) {
                ctx.fake_edata = cur.data();
                ctx.fake_vid = cur.neighbor().GetValue();
                cur.addV(elementSize);
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> defaultContextBase,
            DefaultMessageManager messageManager) {
        TraverseWhileContext ctx = (TraverseWhileContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        for (int i = 0; i < innerVerteicesEnd; ++i) {
            vertex.SetValue((long) i);
            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
            Nbr<Long, Double> cur = adjList.begin();
            // Nbr<Long, Double> cur = fragment.GetOutgoingAdjListBegin(i);
            long elementSize = cur.elementSize();
            long endPointerAddr = adjList.end().getAddress();
            // long endPointerAddr = fragment.GetOutgoingAdjListEnd(i).getAddress();
            while (cur.getAddress() != endPointerAddr) {
                ctx.fake_edata = cur.data();
                ctx.fake_vid = cur.neighbor().GetValue();
                cur.addV(elementSize);
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
