package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.rpc.RpcChannel;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// send rpc to CoordinatorSnapshotService in Store,  to report minimum snapshot current used
public class CoordinatorSnapshotClient extends RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorSnapshotClient.class);

    public CoordinatorSnapshotClient(RpcChannel channel) {
        super(channel);
    }

    private CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceBlockingStub getStub() {
        return CoordinatorSnapshotServiceGrpc.newBlockingStub(rpcChannel.getChannel());
    }

    public void synchronizeSnapshot(long snapshotId) throws RuntimeException {
        SynchronizeMinQuerySnapshotIdRequest req =
                SynchronizeMinQuerySnapshotIdRequest.newBuilder().setSnapshotId(snapshotId).build();
        SynchronizeMinQuerySnapshotIdResponse res = getStub().synchronizeMinQuerySnapshotId(req);
        if (!res.getSuccess()) {
            throw new InvalidArgumentException(
                    "Synchronize snapshot to store failed: " + res.getErrMsg());
        }
    }
}
