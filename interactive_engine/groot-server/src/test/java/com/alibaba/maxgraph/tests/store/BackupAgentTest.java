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
package com.alibaba.maxgraph.tests.store;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.store.BackupAgent;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.StoreBackupId;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.graphscope.groot.store.jna.JnaGraphBackupEngine;
import com.alibaba.graphscope.groot.store.jna.JnaGraphStore;
import com.alibaba.maxgraph.common.config.BackupConfig;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class BackupAgentTest {

    @Test
    void testBackupAgent() throws IOException {
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.NODE_IDX.getKey(), "0")
                        .put(BackupConfig.BACKUP_ENABLE.getKey(), "true")
                        .put(BackupConfig.STORE_BACKUP_THREAD_COUNT.getKey(), "2")
                        .build();
        StoreService mockStoreService = mock(StoreService.class);
        JnaGraphStore mockJnaStore0 = mock(JnaGraphStore.class);
        JnaGraphStore mockJnaStore1 = mock(JnaGraphStore.class);
        JnaGraphBackupEngine mockJnaBackupEngine0 = mock(JnaGraphBackupEngine.class);
        JnaGraphBackupEngine mockJnaBackupEngine1 = mock(JnaGraphBackupEngine.class);
        Map<Integer, GraphPartition> idToPartition = new HashMap<>();
        idToPartition.put(0, mockJnaStore0);
        idToPartition.put(1, mockJnaStore1);
        when(mockStoreService.getIdToPartition()).thenReturn(idToPartition);
        when(mockJnaStore0.openBackupEngine()).thenReturn(mockJnaBackupEngine0);
        when(mockJnaStore1.openBackupEngine()).thenReturn(mockJnaBackupEngine1);

        BackupAgent backupAgent = new BackupAgent(configs, mockStoreService);
        backupAgent.start();

        StoreBackupId storeBackupId = new StoreBackupId(5);
        storeBackupId.addPartitionBackupId(0, 7);
        storeBackupId.addPartitionBackupId(1, 6);
        Map<Integer, List<Integer>> readyPartitionBackupIds = new HashMap<>();
        readyPartitionBackupIds.put(0, Arrays.asList(2, 4, 6));
        readyPartitionBackupIds.put(1, Arrays.asList(3, 5, 7));

        when(mockJnaBackupEngine0.createNewPartitionBackup()).thenReturn(7);
        when(mockJnaBackupEngine1.createNewPartitionBackup()).thenReturn(6);
        CompletionCallback<StoreBackupId> createCallback = mock(CompletionCallback.class);
        backupAgent.createNewStoreBackup(5, createCallback);
        verify(createCallback, timeout(5000L)).onCompleted(storeBackupId);

        CompletionCallback<Void> verifyCallback = mock(CompletionCallback.class);
        backupAgent.verifyStoreBackup(storeBackupId, verifyCallback);
        verify(mockJnaBackupEngine0, timeout(5000L)).verifyPartitionBackup(7);
        verify(mockJnaBackupEngine1, timeout(5000L)).verifyPartitionBackup(6);
        verify(verifyCallback, timeout(5000L)).onCompleted(null);

        CompletionCallback<Void> clearCallback = mock(CompletionCallback.class);
        backupAgent.clearUnavailableStoreBackups(readyPartitionBackupIds, clearCallback);
        verify(mockJnaBackupEngine0, timeout(5000L)).partitionBackupGc(Arrays.asList(2, 4, 6));
        verify(mockJnaBackupEngine1, timeout(5000L)).partitionBackupGc(Arrays.asList(3, 5, 7));
        verify(clearCallback, timeout(5000L)).onCompleted(null);

        CompletionCallback<Void> restoreCallback = mock(CompletionCallback.class);
        backupAgent.restoreFromStoreBackup(storeBackupId, "restore_root", restoreCallback);
        verify(mockJnaBackupEngine0, timeout(5000L)).restoreFromPartitionBackup(
                7, Paths.get("restore_root", "0").toString());
        verify(mockJnaBackupEngine1, timeout(5000L)).restoreFromPartitionBackup(
                6, Paths.get("restore_root", "1").toString());
        verify(restoreCallback, timeout(5000L)).onCompleted(null);

        backupAgent.stop();
    }
}
