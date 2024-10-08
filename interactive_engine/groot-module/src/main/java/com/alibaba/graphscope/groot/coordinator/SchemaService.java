/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.exception.DdlException;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.alibaba.graphscope.proto.groot.SchemaGrpc;
import com.alibaba.graphscope.proto.groot.SubmitBatchDdlRequest;
import com.alibaba.graphscope.proto.groot.SubmitBatchDdlResponse;

import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaService extends SchemaGrpc.SchemaImplBase {
    private static final Logger logger = LoggerFactory.getLogger(SchemaService.class);

    private final SchemaManager schemaManager;

    public SchemaService(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public void submitBatchDdl(
            SubmitBatchDdlRequest request,
            StreamObserver<SubmitBatchDdlResponse> responseObserver) {
        String requestId = request.getRequestId();
        String sessionId = request.getSessionId();
        DdlRequestBatch ddlRequestBatch = DdlRequestBatch.parseProto(request.getDdlRequests());
        logger.info("submitBatchDdl {}", ddlRequestBatch.toProto());
        this.schemaManager.submitBatchDdl(
                requestId,
                sessionId,
                ddlRequestBatch,
                new CompletionCallback<Long>() {
                    @Override
                    public void onCompleted(Long res) {
                        responseObserver.onNext(
                                SubmitBatchDdlResponse.newBuilder()
                                        .setSuccess(true)
                                        .setDdlSnapshotId(res)
                                        .build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        if (t instanceof DdlException) {
                            responseObserver.onNext(
                                    SubmitBatchDdlResponse.newBuilder()
                                            .setSuccess(false)
                                            .setMsg(t.getMessage())
                                            .build());
                            responseObserver.onCompleted();
                        } else {
                            responseObserver.onError(t);
                        }
                    }
                });
    }
}
