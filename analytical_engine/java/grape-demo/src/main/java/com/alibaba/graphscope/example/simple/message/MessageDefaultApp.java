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

package com.alibaba.graphscope.example.simple.message;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

public class MessageDefaultApp
        implements DefaultAppBase<Long, Long, Long, Double, MessageDefaultContext> {
    @Override
    public void PEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> defaultContextBase,
            DefaultMessageManager messageManager) {
        MessageDefaultContext ctx = (MessageDefaultContext) defaultContextBase;
        VertexRange<Long> outerVertices = fragment.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            messageManager.syncStateOnOuterVertex(fragment, vertex, fragment.getData(vertex) + 1);
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase<Long, Long, Long, Double> defaultContextBase,
            DefaultMessageManager messageManager) {
        MessageDefaultContext ctx = (MessageDefaultContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        {
            Long msg = new Long(1L);
            Vertex<Long> curVertex = fragment.innerVertices().begin();
            while (messageManager.getMessage(fragment, curVertex, msg)) {
                // process with the msg
                ctx.numMsgReceived += 1;
            }
            System.out.println("last received msg" + msg);
            ctx.receiveMsgTime += System.nanoTime();
        }
        for (Vertex<Long> vertex : fragment.outerVertices().locals()) {
            messageManager.syncStateOnOuterVertex(fragment, vertex, fragment.getData(vertex) + 1);
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
