package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.proto.v2.IngestorWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.v2.WriteIngestorResponse;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import io.grpc.stub.StreamObserver;

public class IngestorWriteService extends IngestorWriteGrpc.IngestorWriteImplBase {

    private IngestService ingestService;

    public IngestorWriteService(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @Override
    public void writeIngestor(WriteIngestorRequest request, StreamObserver<WriteIngestorResponse> responseObserver) {
        try {
            int queueId = request.getQueueId();
            String requestId = request.getRequestId();
            OperationBatch operationBatch = OperationBatch.parseProto(request.getOperationBatch());
            this.ingestService.ingestBatch(requestId, queueId, operationBatch, new IngestCallback() {
                @Override
                public void onSuccess(long snapshotId) {
                    WriteIngestorResponse response = WriteIngestorResponse.newBuilder()
                            .setSnapshotId(snapshotId)
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }

                @Override
                public void onFailure(Exception e) {
                    responseObserver.onError(e);
                }
            });
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
