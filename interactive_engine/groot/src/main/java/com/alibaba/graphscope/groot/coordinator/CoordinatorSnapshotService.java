package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.maxgraph.proto.groot.CoordinatorSnapshotServiceGrpc;
import com.alibaba.maxgraph.proto.groot.UpdateMinQuerySnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.UpdateMinQuerySnapshotIdResponse;

import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorSnapshotService
        extends CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorSnapshotService.class);
    private GarbageCollectManager garbageCollectManager;

    public CoordinatorSnapshotService(GarbageCollectManager garbageCollectManager) {
        this.garbageCollectManager = garbageCollectManager;
    }

    @Override
    public void updateMinQuerySnapshotId(
            UpdateMinQuerySnapshotIdRequest request,
            StreamObserver<UpdateMinQuerySnapshotIdResponse> responseObserver) {
        int frontendId = request.getFrontendId();
        long snapshotId = request.getSnapshotId();
        garbageCollectManager.put(frontendId, snapshotId);
        UpdateMinQuerySnapshotIdResponse response =
                UpdateMinQuerySnapshotIdResponse.newBuilder().setSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
