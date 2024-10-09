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

package com.alibaba.graphscope.gremlin.result.processor;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.result.ResultParser;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.gremlin.result.GroupResultParser;
import com.alibaba.graphscope.proto.frontend.Code;
import com.alibaba.pegasus.common.StreamIterator;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.collect.Lists;

import io.grpc.Status;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractResultProcessor extends StandardOpProcessor
        implements ResultProcessor {
    protected final Context writeResult;
    protected final ResultParser resultParser;
    protected final QueryStatusCallback statusCallback;
    protected final QueryTimeoutConfig timeoutConfig;
    protected final List<Object> resultCollectors;
    protected final int resultCollectorsBatchSize;
    protected final StreamIterator<PegasusClient.JobResponse> responseStreamIterator;

    protected AbstractResultProcessor(
            Configs configs,
            Context writeResult,
            ResultParser resultParser,
            QueryStatusCallback statusCallback,
            QueryTimeoutConfig timeoutConfig) {
        this.writeResult = writeResult;
        this.resultParser = resultParser;
        this.statusCallback = statusCallback;
        this.timeoutConfig = timeoutConfig;

        RequestMessage msg = writeResult.getRequestMessage();
        Settings settings = writeResult.getSettings();
        // init batch size from resultIterationBatchSize in conf/gremlin-server.yaml,
        // or args in RequestMessage which is originated from gremlin client
        this.resultCollectorsBatchSize =
                (Integer)
                        msg.optionalArgs(Tokens.ARGS_BATCH_SIZE)
                                .orElse(settings.resultIterationBatchSize);
        this.resultCollectors = new ArrayList<>(this.resultCollectorsBatchSize);
        int capacity = FrontendConfig.PER_QUERY_STREAM_BUFFER_MAX_CAPACITY.get(configs);
        this.responseStreamIterator = new StreamIterator<>(capacity);
    }

    @Override
    public synchronized void process(PegasusClient.JobResponse response) {
        try {
            responseStreamIterator.putData(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void finish() {
        try {
            responseStreamIterator.finish();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void error(Status status) {
        responseStreamIterator.fail(status.asException());
    }

    // request results from remote engine service in blocking way
    public void request() {
        try {
            BatchResponseProcessor responseProcessor = new BatchResponseProcessor();
            while (responseStreamIterator.hasNext()) {
                responseProcessor.process(responseStreamIterator.next());
            }
            responseProcessor.finish();
            statusCallback.getQueryLogger().info("[compile]: process results success");
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
            writeResult.writeAndFlush(
                    ResponseMessage.build(writeResult.getRequestMessage())
                            .code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(errorMsg)
                            .create());
        } finally {
            // close the responseStreamIterator so that the subsequent grpc callback do nothing
            // actually
            if (responseStreamIterator != null) {
                responseStreamIterator.close();
            }
        }
    }

    protected abstract void aggregateResults();

    private class BatchResponseProcessor {
        public void process(PegasusClient.JobResponse response) {
            // send back a page of results if batch size is met and then reset the
            // resultCollectors
            if (resultCollectors.size() >= resultCollectorsBatchSize
                    && !(resultParser instanceof GroupResultParser)) {
                aggregateResults();
                writeResult.writeAndFlush(
                        ResponseMessage.build(writeResult.getRequestMessage())
                                .code(ResponseStatusCode.PARTIAL_CONTENT)
                                .result(Lists.newArrayList(resultCollectors))
                                .create());
                resultCollectors.clear();
            }
            resultCollectors.addAll(
                    ClassUtils.callException(
                            () -> resultParser.parseFrom(response), Code.GREMLIN_INVALID_RESULT));
        }

        public void finish() {
            statusCallback.onSuccessEnd();
            aggregateResults();
            writeResult.writeAndFlush(
                    ResponseMessage.build(writeResult.getRequestMessage())
                            .code(ResponseStatusCode.SUCCESS)
                            .result(resultCollectors)
                            .create());
        }
    }
}
