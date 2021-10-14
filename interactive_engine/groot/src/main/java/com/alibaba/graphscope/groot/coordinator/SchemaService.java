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

import com.alibaba.maxgraph.proto.groot.SchemaGrpc;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlRequest;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.schema.request.DdlException;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import io.grpc.stub.StreamObserver;

public class SchemaService extends SchemaGrpc.SchemaImplBase {

    private SchemaManager schemaManager;

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
