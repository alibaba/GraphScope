package com.alibaba.maxgraph.v2.store;

import com.alibaba.maxgraph.proto.v2.*;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import io.grpc.stub.StreamObserver;

public class StoreIngestService extends StoreIngestGrpc.StoreIngestImplBase {

    private StoreService storeService;

    public StoreIngestService(StoreService storeService) {
        this.storeService = storeService;
    }

    @Override
    public void storeIngest(StoreIngestRequest request, StreamObserver<StoreIngestResponse> responseObserver) {
        String dataPath = request.getDataPath();
        this.storeService.ingestData(dataPath, new CompletionCallback<Void>() {
            @Override
            public void onCompleted(Void res) {
                responseObserver.onNext(StoreIngestResponse.newBuilder().build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }
        });
    }
}
