package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.SchemaGrpc;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlRequest;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlResponse;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.schema.request.DdlException;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import io.grpc.stub.StreamObserver;

public class SchemaService extends SchemaGrpc.SchemaImplBase {

    private SchemaManager schemaManager;

    public SchemaService(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public void submitBatchDdl(SubmitBatchDdlRequest request, StreamObserver<SubmitBatchDdlResponse> responseObserver) {
        String requestId = request.getRequestId();
        String sessionId = request.getSessionId();
        DdlRequestBatch ddlRequestBatch = DdlRequestBatch.parseProto(request.getDdlRequests());
        this.schemaManager.submitBatchDdl(requestId, sessionId, ddlRequestBatch,
                new CompletionCallback<Long>() {
            @Override
            public void onCompleted(Long res) {
                responseObserver.onNext(SubmitBatchDdlResponse.newBuilder()
                        .setSuccess(true)
                        .setDdlSnapshotId(res)
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof DdlException) {
                    responseObserver.onNext(SubmitBatchDdlResponse.newBuilder()
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
