package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.proto.v2.StoreDataBatchPb;
import com.alibaba.maxgraph.proto.v2.StoreWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteStoreRequest;
import com.alibaba.maxgraph.proto.v2.WriteStoreResponse;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/**
 * ingestor -> store
 */
public class StoreWriteClient extends RpcClient {

    private StoreWriteGrpc.StoreWriteStub stub;

    public StoreWriteClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreWriteGrpc.newStub(channel);
    }

    public StoreWriteClient(StoreWriteGrpc.StoreWriteStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void writeStore(StoreDataBatch storeDataBatch, CompletionCallback<Integer> callback) {
        StoreDataBatchPb batchPb = storeDataBatch.toProto();
        WriteStoreRequest req = WriteStoreRequest.newBuilder()
                .setBatch(batchPb)
                .build();
        stub.writeStore(req, new StreamObserver<WriteStoreResponse>() {
            @Override
            public void onNext(WriteStoreResponse writeStoreResponse) {
                boolean success = writeStoreResponse.getSuccess();
                if (success) {
                    callback.onCompleted(batchPb.getSerializedSize());
                } else {
                    onError(new RuntimeException("store buffer is full"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                callback.onError(throwable);
            }

            @Override
            public void onCompleted() {
            }
        });
    }

}
