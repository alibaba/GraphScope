package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.maxgraph.proto.groot.*;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreSnapshotService
        extends CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceImplBase {
    private static final Logger logger =
            LoggerFactory.getLogger(com.alibaba.graphscope.groot.store.StoreSnapshotService.class);

    private StoreService storeService;

    public StoreSnapshotService(StoreService storeService) {
        this.storeService = storeService;
    }

    @Override
    public void synchronizeMinQuerySnapshotId(
            SynchronizeMinQuerySnapshotIdRequest request,
            StreamObserver<SynchronizeMinQuerySnapshotIdResponse> responseObserver) {
        long snapshotId = request.getSnapshotId();
        logger.info("synchronizeMinQuerySnapshotId of snapshot [" + snapshotId + "]");
        this.storeService.garbageCollect(
                snapshotId,
                new CompletionCallback<Void>() {
                    @Override
                    public void onCompleted(Void res) {
                        responseObserver.onNext(
                                SynchronizeMinQuerySnapshotIdResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(
                                Status.INTERNAL
                                        .withDescription(t.getMessage())
                                        .asRuntimeException());
                    }
                });
    }
}
