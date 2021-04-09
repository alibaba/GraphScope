package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.proto.v2.SchemaGrpc;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlRequest;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlResponse;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import com.alibaba.maxgraph.v2.common.schema.request.DdlException;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import io.grpc.ManagedChannel;

public class SchemaClient extends RpcClient {

    private SchemaGrpc.SchemaBlockingStub stub;

    public SchemaClient(ManagedChannel channel) {
        super(channel);
        this.stub = SchemaGrpc.newBlockingStub(channel);
    }

    public SchemaClient(SchemaGrpc.SchemaBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public long submitBatchDdl(String requestId, String sessionId, DdlRequestBatch ddlRequestBatch) {
        SubmitBatchDdlRequest request = SubmitBatchDdlRequest.newBuilder()
                .setRequestId(requestId)
                .setSessionId(sessionId)
                .setDdlRequests(ddlRequestBatch.toProto())
                .build();
        SubmitBatchDdlResponse submitBatchDdlResponse = stub.submitBatchDdl(request);
        if (submitBatchDdlResponse.getSuccess()) {
            long ddlSnapshotId = submitBatchDdlResponse.getDdlSnapshotId();
            return ddlSnapshotId;
        } else {
            throw new DdlException(submitBatchDdlResponse.getMsg());
        }
    }

}
