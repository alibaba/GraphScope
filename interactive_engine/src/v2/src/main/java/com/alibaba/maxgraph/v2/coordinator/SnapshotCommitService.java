package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.CommitSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.CommitSnapshotIdResponse;
import com.alibaba.maxgraph.proto.v2.SnapshotCommitGrpc;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class SnapshotCommitService extends SnapshotCommitGrpc.SnapshotCommitImplBase {

    private SnapshotManager snapshotManager;

    public SnapshotCommitService(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void commitSnapshotId(CommitSnapshotIdRequest request, StreamObserver<CommitSnapshotIdResponse> responseObserver) {
        int storeId = request.getStoreId();
        long snapshotId = request.getSnapshotId();
        long ddlSnapshotId = request.getDdlSnapshotId();
        List<Long> queueOffsets = request.getQueueOffsetsList();
        // prevent gRPC auto-cancellation
        Context.current().fork().run(() ->
                this.snapshotManager.commitSnapshotId(storeId, snapshotId, ddlSnapshotId, queueOffsets));
        responseObserver.onNext(CommitSnapshotIdResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
