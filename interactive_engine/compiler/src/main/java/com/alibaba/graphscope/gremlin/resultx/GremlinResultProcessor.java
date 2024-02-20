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
import com.google.common.collect.Maps;

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;

import java.util.List;
import java.util.Map;

public class GremlinResultProcessor implements ExecutionResponseListener {
    protected final Context ctx;
    protected final QueryStatusCallback statusCallback;
    protected final RecordParser<Object> recordParser;
    protected final ResultSchema resultSchema;
    protected final Map<Object, Object> reducer;

    public GremlinResultProcessor(
            Context ctx,
            QueryStatusCallback statusCallback,
            RecordParser recordParser,
            ResultSchema resultSchema) {
        this.ctx = ctx;
        this.statusCallback = statusCallback;
        this.recordParser = recordParser;
        this.resultSchema = resultSchema;
        this.reducer = Maps.newLinkedHashMap();
    }

    @Override
    public void onNext(IrResult.Record record) {
        List<Object> results = recordParser.parseFrom(record);
        if (resultSchema.isGroupBy && !results.isEmpty()) {
            if (results.stream().anyMatch(k -> !(k instanceof Map))) {
                throw new IllegalArgumentException(
                        "cannot reduce results " + results + " into a single map");
            }
            for (Object result : results) {
                reducer.putAll((Map) result);
            }
        } else if (!resultSchema.isGroupBy) {
            ctx.writeAndFlush(
                    ResponseMessage.build(ctx.getRequestMessage())
                            .code(ResponseStatusCode.PARTIAL_CONTENT)
                            .result(results)
                            .create());
        }
    }

    @Override
    public void onCompleted() {
        List<Object> results = Lists.newArrayList();
        if (resultSchema.isGroupBy) {
            results.add(reducer);
        }
        ctx.writeAndFlush(
                ResponseMessage.build(ctx.getRequestMessage())
                        .code(ResponseStatusCode.SUCCESS)
                        .result(results)
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
