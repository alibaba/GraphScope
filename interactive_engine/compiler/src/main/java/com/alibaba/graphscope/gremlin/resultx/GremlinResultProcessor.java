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
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.proto.frontend.Code;
import com.alibaba.pegasus.common.StreamIterator;
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
    protected final StreamIterator<IrResult.Record> recordStreamIterator;
    protected final QueryTimeoutConfig timeoutConfig;

    public GremlinResultProcessor(
            Configs configs,
            Context ctx,
            RecordParser recordParser,
            ResultSchema resultSchema,
            QueryStatusCallback statusCallback,
            QueryTimeoutConfig timeoutConfig) {
        this.ctx = ctx;
        this.recordParser = recordParser;
        this.resultSchema = resultSchema;
        this.statusCallback = statusCallback;
        this.timeoutConfig = timeoutConfig;
        this.reducer = Maps.newLinkedHashMap();
        this.recordStreamIterator =
                new StreamIterator<>(
                        FrontendConfig.PER_QUERY_STREAM_BUFFER_MAX_CAPACITY.get(configs));
    }

    @Override
    public void onNext(IrResult.Record record) {
        try {
            recordStreamIterator.putData(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onCompleted() {
        try {
            recordStreamIterator.finish();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        recordStreamIterator.fail(t);
    }

    public void request() {
        try {
            while (recordStreamIterator.hasNext()) {
                processRecord(recordStreamIterator.next());
            }
            finishRecord();
        } catch (Throwable t) {
            // if the exception is caused by InterruptedException, it means a timeout exception has
            // been thrown by gremlin executor
            Exception executionException =
                    (t != null && t.getCause() instanceof InterruptedException)
                            ? new FrontendException(
                                    Code.TIMEOUT,
                                    ClassUtils.getTimeoutError(
                                            "Timeout has been detected by gremlin executor",
                                            timeoutConfig),
                                    t)
                            : ClassUtils.handleExecutionException(t, timeoutConfig);
            if (executionException instanceof FrontendException) {
                ((FrontendException) executionException)
                        .getDetails()
                        .put("QueryId", statusCallback.getQueryLogger().getQueryId());
            }
            String errorMsg = executionException.getMessage();
            statusCallback.onErrorEnd(executionException, errorMsg);
            ctx.writeAndFlush(
                    ResponseMessage.build(ctx.getRequestMessage())
                            .code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(errorMsg)
                            .create());
        } finally {
            // close the responseStreamIterator so that the subsequent grpc callback do nothing
            // actually
            if (recordStreamIterator != null) {
                recordStreamIterator.close();
            }
        }
    }

    protected void processRecord(IrResult.Record record) {
        List<Object> results =
                ClassUtils.callException(
                        () -> recordParser.parseFrom(record), Code.GREMLIN_INVALID_RESULT);
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

    protected void finishRecord() {
        statusCallback.onSuccessEnd();
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
}
