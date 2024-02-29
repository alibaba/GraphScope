package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.ManagedChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// send rpc to CoordinatorSnapshotService in Store,  to report minimum snapshot current used
public class CoordinatorSnapshotClient extends RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorSnapshotClient.class);
    private final CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceBlockingStub stub;

    public CoordinatorSnapshotClient(ManagedChannel channel) {
        super(channel);
        this.stub = CoordinatorSnapshotServiceGrpc.newBlockingStub(channel);
    }

    public void synchronizeSnapshot(long snapshotId) throws RuntimeException {
        SynchronizeMinQuerySnapshotIdRequest req =
                SynchronizeMinQuerySnapshotIdRequest.newBuilder().setSnapshotId(snapshotId).build();
        SynchronizeMinQuerySnapshotIdResponse res = stub.synchronizeMinQuerySnapshotId(req);
        if (!res.getSuccess()) {
            throw new RuntimeException("Synchronize snapshot to store failed: " + res.getErrMsg());
        }
    }
}
