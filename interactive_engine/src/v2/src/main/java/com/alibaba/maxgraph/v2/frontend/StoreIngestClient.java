package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.proto.v2.StoreIngestGrpc;
import com.alibaba.maxgraph.proto.v2.StoreIngestRequest;
import com.alibaba.maxgraph.proto.v2.StoreIngestResponse;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class StoreIngestClient extends RpcClient {

    private StoreIngestGrpc.StoreIngestStub stub;

    public StoreIngestClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreIngestGrpc.newStub(channel);
    }

    public void storeIngest(String dataPath, CompletionCallback<Void> callback) {
        StoreIngestRequest req = StoreIngestRequest.newBuilder().setDataPath(dataPath).build();
        this.stub.storeIngest(req, new StreamObserver<StoreIngestResponse>() {
            @Override
            public void onNext(StoreIngestResponse value) {
                callback.onCompleted(null);
            }

            @Override
            public void onError(Throwable t) {
                callback.onError(t);
            }

            @Override
            public void onCompleted() {

            }
        });
    }
}
