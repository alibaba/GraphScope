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

package com.alibaba.graphscope.gremlin.resultx;

import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.google.common.collect.Lists;

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;

public class GremlinResultProcessor implements ExecutionResponseListener {
    private final Context ctx;
    private final QueryStatusCallback statusCallback;
    private final RecordParser<Object> recordParser;

    public GremlinResultProcessor(
            Context ctx, QueryStatusCallback statusCallback, RecordParser<Object> recordParser) {
        this.ctx = ctx;
        this.statusCallback = statusCallback;
        this.recordParser = recordParser;
    }

    @Override
    public void onNext(IrResult.Record record) {
        ctx.writeAndFlush(
                ResponseMessage.build(ctx.getRequestMessage())
                        .code(ResponseStatusCode.PARTIAL_CONTENT)
                        .result(parseRecord(record))
                        .create());
    }

    protected Object parseRecord(IrResult.Record record) {
        return recordParser.parseFrom(record);
    }

    @Override
    public void onCompleted() {
        ctx.writeAndFlush(
                ResponseMessage.build(ctx.getRequestMessage())
                        .code(ResponseStatusCode.SUCCESS)
                        .result(Lists.newArrayList())
                        .create());
    }

    @Override
    public void onError(Throwable t) {
        ctx.writeAndFlush(
                ResponseMessage.build(ctx.getRequestMessage())
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusMessage(t.getMessage())
                        .create());
    }
}
