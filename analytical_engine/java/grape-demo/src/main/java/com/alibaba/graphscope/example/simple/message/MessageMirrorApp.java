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
import com.alibaba.graphscope.example.mirror.Edata;
import com.alibaba.graphscope.example.mirror.Message;
import com.alibaba.graphscope.example.mirror.Oid;
import com.alibaba.graphscope.example.mirror.Vdata;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

/**
 * This app demonstrate how to use a FFIMirror as message type. You shall use any FFIMirror in
 * interfaces exposed in MessageManager as you wish, we will provide the template instantiation for
 * the methods you invoked.
 */
public class MessageMirrorApp
        implements DefaultAppBase<Oid, Long, Vdata, Edata, MessageMirrorDefaultContext> {
    @Override
    public void PEval(
            ImmutableEdgecutFragment<Oid, Long, Vdata, Edata> fragment,
            DefaultContextBase<Oid, Long, Vdata, Edata> defaultContextBase,
            DefaultMessageManager messageManager) {
        MessageMirrorDefaultContext ctx = (MessageMirrorDefaultContext) defaultContextBase;
        VertexRange<Long> outerVertices = fragment.outerVertices();
        Message msg = Message.create();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            msg.data(fragment.getData(vertex).data());
            messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            ImmutableEdgecutFragment<Oid, Long, Vdata, Edata> fragment,
            DefaultContextBase<Oid, Long, Vdata, Edata> defaultContextBase,
            DefaultMessageManager messageManager) {
        MessageMirrorDefaultContext ctx = (MessageMirrorDefaultContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        {
            Message msg = Message.create();
            Vertex<Long> curVertex = fragment.innerVertices().begin();
            while (messageManager.getMessage(fragment, curVertex, msg)) {
                // process with the msg
                ctx.numMsgReceived += 1;
            }
            System.out.println("last received msg" + msg.data());
            ctx.receiveMsgTime += System.nanoTime();
        }
        Message msg = Message.create();
        for (Vertex<Long> vertex : fragment.outerVertices().locals()) {
            msg.data(fragment.getData(vertex).data() + 1L);
            messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
