package com.alibaba.maxgraph.v2.store;

import com.alibaba.maxgraph.proto.v2.CommitSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.SnapshotCommitGrpc;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;

import java.util.List;

public class SnapshotCommitClient extends RpcClient {

    private SnapshotCommitGrpc.SnapshotCommitBlockingStub stub;

    public SnapshotCommitClient(ManagedChannel channel) {
        super(channel);
        this.stub = SnapshotCommitGrpc.newBlockingStub(channel);
    }

    public SnapshotCommitClient(SnapshotCommitGrpc.SnapshotCommitBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void commitSnapshotId(int storeId, long snapshotId, long ddlSnapshotId, List<Long> queueOffsets) {
        CommitSnapshotIdRequest req = CommitSnapshotIdRequest.newBuilder()
                .setStoreId(storeId)
                .setSnapshotId(snapshotId)
                .setDdlSnapshotId(ddlSnapshotId)
                .addAllQueueOffsets(queueOffsets)
                .build();
        stub.commitSnapshotId(req);
    }

}
