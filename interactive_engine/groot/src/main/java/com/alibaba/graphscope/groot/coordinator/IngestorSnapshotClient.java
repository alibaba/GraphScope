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

import com.alibaba.maxgraph.proto.groot.IngestorSnapshotGrpc;
import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RpcClient;
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
        AdvanceIngestSnapshotIdRequest req =
                AdvanceIngestSnapshotIdRequest.newBuilder().setSnapshotId(writeSnapshotId).build();
        stub.advanceIngestSnapshotId(
                req,
                new StreamObserver<AdvanceIngestSnapshotIdResponse>() {
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
                    public void onCompleted() {}
                });
    }
}
