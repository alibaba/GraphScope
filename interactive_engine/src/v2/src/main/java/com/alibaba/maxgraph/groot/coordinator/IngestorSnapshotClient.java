/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.groot.coordinator;

import com.alibaba.maxgraph.proto.v2.IngestorSnapshotGrpc;
import com.alibaba.maxgraph.proto.v2.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.AdvanceIngestSnapshotIdResponse;
import com.alibaba.maxgraph.groot.common.CompletionCallback;
import com.alibaba.maxgraph.groot.common.rpc.RpcClient;
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
