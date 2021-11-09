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

package com.alibaba.graphscope.example.simple.wcc;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.AdjList;
import com.alibaba.graphscope.ds.Nbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WCCDefault implements DefaultAppBase<Long, Long, Long, Double, WCCDefaultContext> {
    private static Logger logger = LoggerFactory.getLogger(WCCDefault.class);

    private void PropagateLabelPush(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            WCCDefaultContext ctx,
            DefaultMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (ctx.currModified.get(vertex)) {
                long cid = ctx.comp_id.get(vertex);
                AdjList<Long, Double> adjListv2 = frag.getOutgoingAdjList(vertex);
                for (Nbr<Long, Double> nbr : adjListv2) {
                    Vertex<Long> cur = nbr.neighbor();
                    if (Long.compareUnsigned(ctx.comp_id.get(cur), cid) > 0) {
                        ctx.comp_id.set(cur, cid);
                        ctx.nextModified.set(cur);
                    }
                }
            }
        }
        LongMsg msg = LongMsg.factory.create();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                msg.setData(ctx.comp_id.get(vertex));
                mm.syncStateOnOuterVertex(frag, vertex, msg);
            }
        }
    }

    private void PropagateLabelPull(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            WCCDefaultContext ctx,
            DefaultMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();

        for (Vertex<Long> cur : innerVertices) {
            long oldCid = ctx.comp_id.get(cur);
            long newCid = oldCid;
            AdjList<Long, Double> nbrs = frag.getOutgoingInnerVertexAdjList(cur);
            for (Nbr<Long, Double> nbr : nbrs) {
                long value = ctx.comp_id.get(nbr.neighbor());
                if (Long.compareUnsigned(value, newCid) < 0) {
                    newCid = value;
                }
            }
            if (Long.compareUnsigned(newCid, oldCid) < 0) {
                ctx.comp_id.set(cur, newCid);
                ctx.nextModified.set(cur);
            }
        }
        LongMsg msg = LongMsg.factory.create();
        for (Vertex<Long> cur : outerVertices) {
            long oldCid = ctx.comp_id.get(cur);
            long newCid = oldCid;
            AdjList<Long, Double> nbrs = frag.getIncomingAdjList(cur);
            for (Nbr<Long, Double> nbr : nbrs) {
                long value = ctx.comp_id.get(nbr.neighbor());
                if (Long.compareUnsigned(value, newCid) < 0) {
                    newCid = value;
                }
            }
            if (Long.compareUnsigned(newCid, oldCid) < 0) {
                ctx.comp_id.set(cur, newCid);
                ctx.nextModified.set(cur);
                msg.setData(newCid);
                mm.syncStateOnOuterVertex(frag, cur, msg);
            }
        }
    }

    @Override
    public void PEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        WCCDefaultContext ctx = (WCCDefaultContext) context;
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            ctx.comp_id.set(vertex, frag.getInnerVertexGid(vertex));
        }
        for (Vertex<Long> vertex : outerVertices.locals()) {
            ctx.comp_id.set(vertex, frag.getOuterVertexGid(vertex));
        }
        // difference between propagateLabel is no currModified check
        PropagateLabelPull(frag, ctx, messageManager);

        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        ctx.currModified.assign(ctx.nextModified);
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultContextBase<Long, Long, Long, Double> context,
            DefaultMessageManager messageManager) {
        WCCDefaultContext ctx = (WCCDefaultContext) context;
        ctx.nextModified.clear();
        { // aggregate message
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            LongMsg msg = LongMsg.factory.create();
            while (messageManager.getMessage(frag, vertex, msg)) {
                if (Long.compareUnsigned(ctx.comp_id.get(vertex), msg.getData()) > 0) {
                    ctx.comp_id.set(vertex, msg.getData());
                    ctx.currModified.set(vertex);
                }
            }
        }
        // iteration

        double rate = (double) ctx.currModified.getBitSet().cardinality() / ctx.innerVerticesNum;
        if (rate > 0.1) {
            PropagateLabelPull(frag, ctx, messageManager);
        } else {
            PropagateLabelPush(frag, ctx, messageManager);
        }

        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        ctx.currModified.assign(ctx.nextModified);
    }
}
