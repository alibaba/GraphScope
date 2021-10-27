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
package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.maxgraph.proto.groot.*;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreBackupService extends StoreBackupGrpc.StoreBackupImplBase {
    private BackupAgent backupAgent;

    public StoreBackupService(BackupAgent backupAgent) {
        this.backupAgent = backupAgent;
    }

    @Override
    public void createStoreBackup(CreateStoreBackupRequest request,
                                  StreamObserver<CreateStoreBackupResponse> responseObserver) {
        this.backupAgent.createNewStoreBackup(request.getGlobalBackupId(), new CompletionCallback<StoreBackupId>() {
            @Override
            public void onCompleted(StoreBackupId res) {
                responseObserver.onNext(CreateStoreBackupResponse.newBuilder().setStoreBackupId(res.toProto()).build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }
        });
    }

    @Override
    public void verifyStoreBackup(VerifyStoreBackupRequest request,
                                  StreamObserver<VerifyStoreBackupResponse> responseObserver) {
        this.backupAgent.verifyStoreBackup(StoreBackupId.parseProto(request.getStoreBackupId()), new CompletionCallback<Void>() {
            @Override
            public void onCompleted(Void res) {
                responseObserver.onNext(VerifyStoreBackupResponse.newBuilder().build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }
        });
    }

    @Override
    public void clearUnavailableStoreBackups(ClearUnavailableStoreBackupsRequest request,
                                        StreamObserver<ClearUnavailableStoreBackupsResponse> responseObserver) {
        Map<Integer, List<Integer>> readyPartitionBackupIds = new HashMap<>(request.getPartitionToReadyBackupIdsCount());
        for (Map.Entry<Integer, PartitionBackupIdListPb> entry : request.getPartitionToReadyBackupIdsMap().entrySet()) {
            readyPartitionBackupIds.put(entry.getKey(), entry.getValue().getReadyPartitionBackupIdsList());
        }
        this.backupAgent.clearUnavailableStoreBackups(readyPartitionBackupIds, new CompletionCallback<Void>() {
            @Override
            public void onCompleted(Void res) {
                responseObserver.onNext(ClearUnavailableStoreBackupsResponse.newBuilder().build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }
        });
    }

    @Override
    public void restoreFromStoreBackup(RestoreFromStoreBackupRequest request,
                                       StreamObserver<RestoreFromStoreBackupResponse> responseObserver) {
        this.backupAgent.restoreFromStoreBackup(
                StoreBackupId.parseProto(request.getStoreBackupId()),
                request.getRestoreRootPath(),
                new CompletionCallback<Void>() {
                    @Override
                    public void onCompleted(Void res) {
                        responseObserver.onNext(RestoreFromStoreBackupResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(t);
                    }
                }
        );
    }

}
