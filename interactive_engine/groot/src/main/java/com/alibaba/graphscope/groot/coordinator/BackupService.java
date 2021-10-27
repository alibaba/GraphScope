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

import com.alibaba.maxgraph.compiler.api.exception.BackupException;
import com.alibaba.maxgraph.proto.groot.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class BackupService extends BackupGrpc.BackupImplBase {
    private BackupManager backupManager;

    public BackupService(BackupManager backupManager) {
        this.backupManager = backupManager;
    }

    @Override
    public void createNewBackup(CreateNewBackupRequest request, StreamObserver<CreateNewBackupResponse> responseObserver) {
        try {
            int newGlobalBackupId = this.backupManager.createNewBackup();
            responseObserver.onNext(CreateNewBackupResponse.newBuilder().setGlobalBackupId(newGlobalBackupId).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteBackup(DeleteBackupRequest request, StreamObserver<DeleteBackupResponse> responseObserver) {
        try {
            this.backupManager.deleteBackup(request.getGlobalBackupId());
            responseObserver.onNext(DeleteBackupResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void purgeOldBackups(PurgeOldBackupsRequest request, StreamObserver<PurgeOldBackupsResponse> responseObserver) {
        try {
            this.backupManager.purgeOldBackups(request.getKeepAliveNumber());
            responseObserver.onNext(PurgeOldBackupsResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void restoreFromLatest(RestoreFromLatestRequest request, StreamObserver<RestoreFromLatestResponse> responseObserver) {
        try {
            this.backupManager.restoreFromLatest(request.getRestoreRootPath());
            responseObserver.onNext(RestoreFromLatestResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void verifyBackup(VerifyBackupRequest request, StreamObserver<VerifyBackupResponse> responseObserver) {
        try {
            this.backupManager.verifyBackup(request.getGlobalBackupId());
            responseObserver.onNext(VerifyBackupResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getBackupInfo(GetBackupInfoRequest request, StreamObserver<GetBackupInfoResponse> responseObserver) {
        List<BackupInfoPb> infoList = this.backupManager.getBackupInfoList()
                .stream()
                .map(BackupInfo::toProto)
                .collect(Collectors.toList());
        responseObserver.onNext(GetBackupInfoResponse.newBuilder().addAllBackupInfoList(infoList).build());
        responseObserver.onCompleted();
    }
}
