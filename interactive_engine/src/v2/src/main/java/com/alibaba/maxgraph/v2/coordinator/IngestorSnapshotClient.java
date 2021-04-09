package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.IngestorSnapshotGrpc;
import com.alibaba.maxgraph.proto.v2.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.AdvanceIngestSnapshotIdResponse;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class IngestorSnapshotClient extends RpcClient {
    private IngestorSnapshotGrpc.IngestorSnapshotStub stub;

    public IngestorSnapshotClient(ManagedChannel channel) {
        super(channel);
        this.stub = IngestorSnapshotGrpc.newStub(channel);
    }

    public IngestorSnapshotClient(IngestorSnapshotGrpc.IngestorSnapshotStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void advanceIngestSnapshotId(long writeSnapshotId, CompletionCallback<Long> callback) {
        AdvanceIngestSnapshotIdRequest req = AdvanceIngestSnapshotIdRequest.newBuilder()
                .setSnapshotId(writeSnapshotId)
                .build();
        stub.advanceIngestSnapshotId(req, new StreamObserver<AdvanceIngestSnapshotIdResponse>() {
            @Override
            public void onNext(AdvanceIngestSnapshotIdResponse response) {
                long previousSnapshotId = response.getPreviousSnapshotId();
                callback.onCompleted(previousSnapshotId);
            }

            @Override
            public void onError(Throwable throwable) {
                callback.onError(throwable);
            }

            @Override
            public void onCompleted() {
            }
        });
    }
}
