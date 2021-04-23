package com.alibaba.maxgraph.v2.store;

import com.alibaba.maxgraph.proto.v2.StoreDataBatchPb;
import com.alibaba.maxgraph.proto.v2.StoreWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteStoreRequest;
import com.alibaba.maxgraph.proto.v2.WriteStoreResponse;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import io.grpc.stub.StreamObserver;

public class StoreWriteService extends StoreWriteGrpc.StoreWriteImplBase {

    private WriterAgent writerAgent;

    public StoreWriteService(WriterAgent writerAgent) {
        this.writerAgent = writerAgent;
    }

    @Override
    public void writeStore(WriteStoreRequest request, StreamObserver<WriteStoreResponse> responseObserver) {
        StoreDataBatchPb batchProto = request.getBatch();
        try {
            boolean success = writerAgent.writeStore(StoreDataBatch.parseProto(batchProto));
            WriteStoreResponse response = WriteStoreResponse.newBuilder()
                    .setSuccess(success)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
