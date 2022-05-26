package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.maxgraph.proto.groot.*;

import io.grpc.ManagedChannel;

// send rpc to CoordinatorSnapshotService in Store,  to report minimum snapshot current used
public class CoordinatorSnapshotClient extends RpcClient {
    private CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceBlockingStub stub;

    public CoordinatorSnapshotClient(ManagedChannel channel) {
        super(channel);
        this.stub = CoordinatorSnapshotServiceGrpc.newBlockingStub(channel);
    }

    public CoordinatorSnapshotClient(
            CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void synchronizeSnapshot(long snapshotId) throws RuntimeException {
        SynchronizeMinQuerySnapshotIdRequest req =
                SynchronizeMinQuerySnapshotIdRequest.newBuilder().setSnapshotId(snapshotId).build();
        SynchronizeMinQuerySnapshotIdResponse res = stub.synchronizeMinQuerySnapshotId(req);
        if (!res.getSuccess()) {
            throw new RuntimeException("Update store snapshot fail {} " + res.getErrMsg());
        }
    }
}
