package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.GetTailOffsetsRequest;
import com.alibaba.maxgraph.proto.v2.GetTailOffsetsResponse;
import com.alibaba.maxgraph.proto.v2.IngestProgressGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IngestProgressService extends IngestProgressGrpc.IngestProgressImplBase {
    private static final Logger logger = LoggerFactory.getLogger(IngestProgressService.class);

    private SnapshotManager snapshotManager;

    public IngestProgressService(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void getTailOffsets(GetTailOffsetsRequest request, StreamObserver<GetTailOffsetsResponse> responseObserver) {
        logger.info("Get offset of [" + request.getQueueIdList() + "]");
        List<Integer> queueIdList = request.getQueueIdList();
        List<Long> tailOffsets = this.snapshotManager.getTailOffsets(queueIdList);
        GetTailOffsetsResponse response = GetTailOffsetsResponse.newBuilder()
                .addAllOffsets(tailOffsets)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
