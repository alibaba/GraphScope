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
package com.alibaba.graphscope.groot.backup;

import com.alibaba.maxgraph.groot.common.CompletionCallback;
import com.alibaba.maxgraph.groot.common.backup.StoreBackupId;
import com.alibaba.maxgraph.groot.common.rpc.RpcClient;
import com.alibaba.maxgraph.proto.v2.CreateStoreBackupRequest;
import com.alibaba.maxgraph.proto.v2.CreateStoreBackupResponse;
import com.alibaba.maxgraph.proto.v2.StoreBackupGrpc;
import com.alibaba.maxgraph.proto.v2.StoreBackupIdPb;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class StoreBackupClient extends RpcClient {
    private StoreBackupGrpc.StoreBackupStub stub;

    public StoreBackupClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreBackupGrpc.newStub(channel);
    }

    public StoreBackupClient(StoreBackupGrpc.StoreBackupStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void createStoreBackup(int globalBackupId, CompletionCallback<StoreBackupId> callback) {
        CreateStoreBackupRequest req = CreateStoreBackupRequest.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .build();
        stub.createStoreBackup(req, new StreamObserver<CreateStoreBackupResponse>() {
            @Override
            public void onNext(CreateStoreBackupResponse response) {
                StoreBackupIdPb finishedStoreBackupIdPb = response.getStoreBackupId();
                callback.onCompleted(StoreBackupId.parseProto(finishedStoreBackupIdPb));
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
