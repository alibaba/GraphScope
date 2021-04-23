package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.AdvanceQuerySnapshotRequest;
import com.alibaba.maxgraph.proto.v2.AdvanceQuerySnapshotResponse;
import com.alibaba.maxgraph.proto.v2.FrontendSnapshotGrpc;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrontendSnapshotClient extends RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(FrontendSnapshotClient.class);

    private FrontendSnapshotGrpc.FrontendSnapshotStub stub;

    public FrontendSnapshotClient(ManagedChannel channel) {
        super(channel);
        this.stub = FrontendSnapshotGrpc.newStub(this.channel);
    }

    public FrontendSnapshotClient(FrontendSnapshotGrpc.FrontendSnapshotStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void advanceQuerySnapshot(long querySnapshotId, GraphDef graphDef, CompletionCallback<Long> callback) {
        AdvanceQuerySnapshotRequest.Builder builder = AdvanceQuerySnapshotRequest.newBuilder();
        builder.setSnapshotId(querySnapshotId);
        if (graphDef != null) {
            builder.setGraphDef(graphDef.toProto());
        }
        stub.advanceQuerySnapshot(builder.build(), new StreamObserver<AdvanceQuerySnapshotResponse>() {
            @Override
            public void onNext(AdvanceQuerySnapshotResponse response) {
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
