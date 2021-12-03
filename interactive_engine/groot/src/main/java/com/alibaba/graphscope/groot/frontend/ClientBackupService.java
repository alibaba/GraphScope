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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.maxgraph.proto.groot.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClientBackupService extends ClientBackupGrpc.ClientBackupImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ClientBackupService.class);

    private RoleClients<BackupClient> backupClients;

    public ClientBackupService(RoleClients<BackupClient> backupClients) {
        this.backupClients = backupClients;
    }

    @Override
    public void createNewGraphBackup(CreateNewGraphBackupRequest request,
                                     StreamObserver<CreateNewGraphBackupResponse> responseObserver) {
        try {
            int newBackupId = backupClients.getClient(0).createNewBackup();
            responseObserver.onNext(CreateNewGraphBackupResponse.newBuilder().setBackupId(newBackupId).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("create new graph backup failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void deleteGraphBackup(DeleteGraphBackupRequest request,
                                  StreamObserver<DeleteGraphBackupResponse> responseObserver) {
        int backupId = request.getBackupId();
        try {
            backupClients.getClient(0).deleteBackup(backupId);
            responseObserver.onNext(DeleteGraphBackupResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("delete graph backup #[" + backupId + "] failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void purgeOldGraphBackups(PurgeOldGraphBackupsRequest request,
                                     StreamObserver<PurgeOldGraphBackupsResponse> responseObserver) {
        int keepAliveNum = request.getKeepAliveNumber();
        try {
            backupClients.getClient(0).purgeOldBackups(keepAliveNum);
            responseObserver.onNext(PurgeOldGraphBackupsResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("purge old graph backups failed, keep alive num = " + keepAliveNum, e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void restoreFromGraphBackup(RestoreFromGraphBackupRequest request,
                                       StreamObserver<RestoreFromGraphBackupResponse> responseObserver) {
        int backupId = request.getBackupId();
        String metaRestorePath = request.getMetaRestorePath();
        String storeRestorePath = request.getStoreRestorePath();
        try {
            backupClients.getClient(0).restoreFromBackup(backupId, metaRestorePath, storeRestorePath);
            responseObserver.onNext(RestoreFromGraphBackupResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("restore from graph backup [" + backupId + "] failed, meta restore path ["
                    + metaRestorePath + "], store restore path [" + storeRestorePath + "]", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void verifyGraphBackup(VerifyGraphBackupRequest request,
                                  StreamObserver<VerifyGraphBackupResponse> responseObserver) {
        int backupId = request.getBackupId();
        try {
            backupClients.getClient(0).verifyBackup(backupId);
            responseObserver.onNext(VerifyGraphBackupResponse.newBuilder().setIsOk(true).setErrMsg("").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("verification for backup #[" + backupId + "] failed", e);
            responseObserver.onNext(VerifyGraphBackupResponse.newBuilder().setIsOk(false).setErrMsg(e.getMessage()).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getGraphBackupInfo(GetGraphBackupInfoRequest request,
                                   StreamObserver<GetGraphBackupInfoResponse> responseObserver) {
        try {
            List<BackupInfoPb> infoPbList = backupClients.getClient(0).getBackupInfo();
            responseObserver.onNext(GetGraphBackupInfoResponse.newBuilder().addAllBackupInfoList(infoPbList).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("get graph backup info failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
