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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.groot.store.StoreBackupId;
import com.alibaba.maxgraph.proto.groot.*;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public void clearUnavailableBackups(Map<Integer, List<Integer>> readyPartitionBackupIdsList,
                                        CompletionCallback<Void> callback) {
        Map<Integer, PartitionBackupIdListPb> partitionToBackupIdListPb =
                new HashMap<>(readyPartitionBackupIdsList.size());
        for (Map.Entry<Integer, List<Integer>> entry : readyPartitionBackupIdsList.entrySet()) {
            partitionToBackupIdListPb.put(
                    entry.getKey(),
                    PartitionBackupIdListPb.newBuilder().addAllReadyPartitionBackupIds(entry.getValue()).build());
        }
        ClearUnavailableStoreBackupsRequest req = ClearUnavailableStoreBackupsRequest.newBuilder()
                .putAllPartitionToReadyBackupIds(partitionToBackupIdListPb)
                .build();
        stub.clearUnavailableStoreBackups(req, new StreamObserver<ClearUnavailableStoreBackupsResponse>() {
            @Override
            public void onNext(ClearUnavailableStoreBackupsResponse clearUnavailableStoreBackupsResponse) {
                callback.onCompleted(null);
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

    public void restoreFromStoreBackup(StoreBackupId storeBackupId, String storeRestoreRootPath,
                                       CompletionCallback<Void> callback) {
        RestoreFromStoreBackupRequest req = RestoreFromStoreBackupRequest.newBuilder()
                .setStoreBackupId(storeBackupId.toProto())
                .setRestoreRootPath(storeRestoreRootPath)
                .build();
        stub.restoreFromStoreBackup(req, new StreamObserver<RestoreFromStoreBackupResponse>() {
            @Override
            public void onNext(RestoreFromStoreBackupResponse restoreFromStoreBackupResponse) {
                callback.onCompleted(null);
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

    public void verifyStoreBackup(StoreBackupId storeBackupId, CompletionCallback<Void> callback) {
        VerifyStoreBackupRequest req = VerifyStoreBackupRequest.newBuilder()
                .setStoreBackupId(storeBackupId.toProto())
                .build();
        stub.verifyStoreBackup(req, new StreamObserver<VerifyStoreBackupResponse>() {
            @Override
            public void onNext(VerifyStoreBackupResponse verifyStoreBackupResponse) {
                callback.onCompleted(null);
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
