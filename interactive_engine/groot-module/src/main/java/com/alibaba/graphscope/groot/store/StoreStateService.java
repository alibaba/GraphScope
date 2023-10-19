package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.proto.groot.GetStoreStateRequest;
import com.alibaba.graphscope.proto.groot.GetStoreStateResponse;
import com.alibaba.graphscope.proto.groot.PartitionStatePb;
import com.alibaba.graphscope.proto.groot.StateServiceGrpc;

import io.grpc.stub.StreamObserver;

public class StoreStateService extends StateServiceGrpc.StateServiceImplBase {
    private StoreService storeService;

    public StoreStateService(StoreService storeService) {
        this.storeService = storeService;
    }

    @Override
    public void getState(
            GetStoreStateRequest request, StreamObserver<GetStoreStateResponse> responseObserver) {
        long[] spaces = this.storeService.getDiskStatus();
        GetStoreStateResponse.Builder builder = GetStoreStateResponse.newBuilder();
        PartitionStatePb state =
                PartitionStatePb.newBuilder()
                        .setTotalSpace(spaces[0])
                        .setUsableSpace(spaces[1])
                        .build();

        builder.putPartitionStates(storeService.getStoreId(), state);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
