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

import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.maxgraph.proto.groot.*;
import io.grpc.ManagedChannel;

import java.util.List;

public class BackupClient extends RpcClient {
    private BackupGrpc.BackupBlockingStub stub;

    public BackupClient(ManagedChannel channel) {
        super(channel);
        this.stub = BackupGrpc.newBlockingStub(channel);
    }

    public BackupClient(BackupGrpc.BackupBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public int createNewBackup() {
        CreateNewBackupRequest request = CreateNewBackupRequest.newBuilder().build();
        CreateNewBackupResponse response =  this.stub.createNewBackup(request);
        return response.getGlobalBackupId();
    }

    public void deleteBackup(int globalBackupId) {
        DeleteBackupRequest request = DeleteBackupRequest.newBuilder().setGlobalBackupId(globalBackupId).build();
        this.stub.deleteBackup(request);
    }

    public void purgeOldBackups(int keepAliveNum) {
        PurgeOldBackupsRequest request = PurgeOldBackupsRequest.newBuilder().setKeepAliveNumber(keepAliveNum).build();
        this.stub.purgeOldBackups(request);
    }

    public void restoreFromBackup(int globalBackupId, String metaRestorePath, String storeRestorePath) {
        RestoreFromBackupRequest request = RestoreFromBackupRequest.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .setMetaRestorePath(metaRestorePath)
                .setStoreRestorePath(storeRestorePath)
                .build();
        this.stub.restoreFromBackup(request);
    }

    public void verifyBackup(int globalBackupId) {
        VerifyBackupRequest request = VerifyBackupRequest.newBuilder().setGlobalBackupId(globalBackupId).build();
        this.stub.verifyBackup(request);
    }

    public List<BackupInfoPb> getBackupInfo() {
        GetBackupInfoRequest request = GetBackupInfoRequest.newBuilder().build();
        GetBackupInfoResponse response = this.stub.getBackupInfo(request);
        return response.getBackupInfoListList();
    }
}
