/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.result;

import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;

import io.grpc.Status;

import org.apache.calcite.rel.RelNode;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;

public class CypherResultProcessor extends StandardOpProcessor implements ResultProcessor {
    private final Context context;
    private final RelNode topNode;

    public CypherResultProcessor(Context context, RelNode topNode) {
        this.context = context;
        this.topNode = topNode;
    }

    @Override
    public void process(PegasusClient.JobResponse jobResponse) {}

    @Override
    public void finish() {
        context.writeAndFlush(
                ResponseMessage.build(context.getRequestMessage())
                        .code(ResponseStatusCode.SUCCESS)
                        .create());
    }

    @Override
    public void error(Status status) {
        context.writeAndFlush(
                ResponseMessage.build(context.getRequestMessage())
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusMessage(status.toString())
                        .create());
    }
}
