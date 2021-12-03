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
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.store.StoreBackupId;

import java.util.List;
import java.util.Map;

public class StoreBackupTaskSender {
    private RoleClients<StoreBackupClient> storeBackupClients;

    public StoreBackupTaskSender(RoleClients<StoreBackupClient> storeBackupClients) {
        this.storeBackupClients = storeBackupClients;
    }

    public void createStoreBackup(int storeId, int globalBackupId, CompletionCallback<StoreBackupId> callback) {
        this.storeBackupClients.getClient(storeId).createStoreBackup(globalBackupId, callback);
    }

    public void clearUnavailableBackups(int storeId, Map<Integer, List<Integer>> readyPartitionBackupIdsList,
                                        CompletionCallback<Void> callback) {
        this.storeBackupClients.getClient(storeId).clearUnavailableBackups(readyPartitionBackupIdsList, callback);
    }

    public void restoreFromStoreBackup(int storeId, StoreBackupId storeBackupId, String storeRestoreRootPath,
                                       CompletionCallback<Void> callback) {
        this.storeBackupClients.getClient(storeId).restoreFromStoreBackup(storeBackupId, storeRestoreRootPath, callback);
    }

    public void verifyStoreBackup(int storeId, StoreBackupId storeBackupId, CompletionCallback<Void> callback) {
        this.storeBackupClients.getClient(storeId).verifyStoreBackup(storeBackupId, callback);
    }
}
