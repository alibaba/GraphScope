package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.proto.v2.IngestorWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.v2.WriteIngestorResponse;
import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;

public class IngestorWriteClient extends RpcClient {

    private IngestorWriteGrpc.IngestorWriteBlockingStub stub;

    public IngestorWriteClient(ManagedChannel channel) {
        super(channel);
        this.stub = IngestorWriteGrpc.newBlockingStub(channel);
    }

    public IngestorWriteClient(IngestorWriteGrpc.IngestorWriteBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public BatchId writeIngestor(String requestId, int queueId, OperationBatch operationBatch) {
        WriteIngestorRequest request = WriteIngestorRequest.newBuilder()
                .setRequestId(requestId)
                .setQueueId(queueId)
                .setOperationBatch(operationBatch.toProto())
                .build();
        WriteIngestorResponse response = this.stub.writeIngestor(request);
        return new BatchId(response.getSnapshotId());
    }
}
