/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotResponse;
import com.alibaba.maxgraph.proto.groot.FrontendSnapshotGrpc;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.groot.schema.GraphDef;
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

    public void advanceQuerySnapshot(
            long querySnapshotId, GraphDef graphDef, CompletionCallback<Long> callback) {
        AdvanceQuerySnapshotRequest.Builder builder = AdvanceQuerySnapshotRequest.newBuilder();
        builder.setSnapshotId(querySnapshotId);
        if (graphDef != null) {
            builder.setGraphDef(graphDef.toProto());
        }
        stub.advanceQuerySnapshot(
                builder.build(),
                new StreamObserver<AdvanceQuerySnapshotResponse>() {
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
                    public void onCompleted() {}
                });
    }
}
