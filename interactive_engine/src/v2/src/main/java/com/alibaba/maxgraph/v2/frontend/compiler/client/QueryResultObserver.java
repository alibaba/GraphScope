/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend.compiler.client;

import com.alibaba.maxgraph.proto.v2.QueryResponse;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphResultProcessor;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.ResultParserUtils;
import com.alibaba.maxgraph.v2.frontend.exception.ExceptionHolder;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class QueryResultObserver implements StreamObserver<QueryResponse> {
    private static final Logger logger = LoggerFactory.getLogger(QueryResultObserver.class);
    private String queryId;
    private CountDownLatch countDownLatch;
    private MaxGraphResultProcessor maxGraphResultProcessor;
    private ExceptionHolder exceptionHolder;
    private boolean receiveFlag;
    private Map<Integer, String> labelIdNameMapping;
    private GraphSchema graphSchema;

    public QueryResultObserver(String queryId,
                               CountDownLatch countDownLatch,
                               MaxGraphResultProcessor maxGraphResultProcessor,
                               ExceptionHolder exceptionHolder,
                               Map<Integer, String> labelIdNameMapping,
                               GraphSchema graphSchema) {
        this.queryId = queryId;
        this.countDownLatch = countDownLatch;
        this.maxGraphResultProcessor = maxGraphResultProcessor;
        this.exceptionHolder = exceptionHolder;
        this.receiveFlag = false;
        this.labelIdNameMapping = labelIdNameMapping;
        this.graphSchema = graphSchema;
    }

    @Override
    public synchronized void onNext(QueryResponse queryResponse) {
        if (queryResponse.getErrorCode() != 0) {
            String errorMessage = "errorCode[" + queryResponse.getErrorCode() + "] errorMessage[" + queryResponse.getMessage() + "]";
            Exception exception = new RuntimeException(errorMessage);
            logger.error("query fail", exception);
            exceptionHolder.hold(exception);
            return;
        }

        List<ByteString> valueList = queryResponse.getValueList();
        for (ByteString bytes : valueList) {
            if (!receiveFlag) {
                receiveFlag = true;
                logger.info("Start to receive and process result for query " + queryId);
            }
            try {
                this.maxGraphResultProcessor.process(ResultParserUtils.parseResponse(bytes, this.graphSchema, this.labelIdNameMapping));
            } catch (Exception e) {
                logger.error("parse value result fail", e);
                exceptionHolder.hold(e);
            }
        }
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        logger.error("query" + this.queryId + " execute failed", throwable);
        countDownLatch.countDown();
    }

    @Override
    public synchronized void onCompleted() {
        countDownLatch.countDown();
    }
}
