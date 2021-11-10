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
package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.SnapshotWithSchema;
import com.alibaba.graphscope.groot.coordinator.*;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.store.StoreBackupId;
import com.alibaba.maxgraph.common.config.BackupConfig;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.alibaba.graphscope.groot.coordinator.BackupManager.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class BackupManagerTest {

    @Test
    void testBackupManager() throws IOException, InterruptedException {
        // init config
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.STORE_NODE_COUNT.getKey(), "2")
                .put(BackupConfig.BACKUP_ENABLE.getKey(), "true")
                .put(BackupConfig.BACKUP_CREATION_BUFFER_MAX_COUNT.getKey(), "4")
                .put(BackupConfig.BACKUP_GC_INTERVAL_HOURS.getKey(), "24")
                .put(BackupConfig.BACKUP_AUTO_SUBMIT.getKey(), "false")
                .put(BackupConfig.BACKUP_AUTO_SUBMIT_INTERVAL_HOURS.getKey(), "24")
                .build();

        // init data
        long querySnapshotId = 10L;
        GraphDef graphDef = GraphDef.newBuilder().setVersion(10L).build();
        SnapshotWithSchema snapshotWithSchema = new SnapshotWithSchema(querySnapshotId, graphDef);
        List<Long> queueOffsets = new ArrayList<>();
        Map<Integer, Integer> partitionToBackupId1 = new HashMap<>();
        partitionToBackupId1.put(0, 1);
        partitionToBackupId1.put(1, 1);
        Map<Integer, Integer> partitionToBackupId2 = new HashMap<>();
        partitionToBackupId2.put(0, 2);
        partitionToBackupId2.put(1, 2);
        BackupInfo backupInfo1 = new BackupInfo(
                1, querySnapshotId, graphDef.toProto().toByteArray(), queueOffsets, partitionToBackupId1);
        BackupInfo backupInfo2 = new BackupInfo(
                2, querySnapshotId, graphDef.toProto().toByteArray(), queueOffsets, partitionToBackupId2);

        // mock MetaStore behaviours
        ObjectMapper objectMapper = new ObjectMapper();
        MetaStore mockMetaStore = mock(MetaStore.class);
        when(mockMetaStore.exists(anyString())).thenReturn(true);
        when(mockMetaStore.read(GLOBAL_BACKUP_ID_PATH)).thenReturn(
                objectMapper.writeValueAsBytes(0));
        when(mockMetaStore.read(BACKUP_INFO_PATH)).thenReturn(
                objectMapper.writeValueAsBytes(new ArrayList<BackupInfo>()));

        // mock MetaService behaviours
        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getPartitionCount()).thenReturn(2);
        when(mockMetaService.getStoreIdByPartition(0)).thenReturn(0);
        when(mockMetaService.getStoreIdByPartition(1)).thenReturn(1);

        // mock SnapshotManager behaviours
        SnapshotManager mockSnapshotManager = mock(SnapshotManager.class);
        when(mockSnapshotManager.getQueueOffsets()).thenReturn(queueOffsets);

        // mock SchemaManager
        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        when(mockSchemaManager.getGraphDef()).thenReturn(graphDef);

        // mock SnapshotCache
        SnapshotCache mockSnapshotCache = mock(SnapshotCache.class);
        when(mockSnapshotCache.getSnapshotWithSchema()).thenReturn(snapshotWithSchema);

        // mock StoreBackupTaskSender behaviours
        StoreBackupTaskSender mockStoreBackupTaskSender = mock(StoreBackupTaskSender.class);
        doAnswer(
                        invocation -> {
                            int partitionOrStoreId = invocation.getArgument(0);
                            int globalBackupId = invocation.getArgument(1);
                            CompletionCallback<StoreBackupId> callback = invocation.getArgument(2);
                            StoreBackupId storeBackupId = new StoreBackupId(globalBackupId);
                            storeBackupId.addPartitionBackupId(partitionOrStoreId, globalBackupId);
                            callback.onCompleted(storeBackupId);
                            return null;
                        })
                .when(mockStoreBackupTaskSender)
                .createStoreBackup(anyInt(), anyInt(), any());
        doAnswer(
                        invocation -> {
                            CompletionCallback<Void> callback = invocation.getArgument(3);
                            callback.onCompleted(null);
                            return null;
                        })
                .when(mockStoreBackupTaskSender)
                .restoreFromStoreBackup(anyInt(), any(), anyString(), any());
        doAnswer(
                        invocation -> {
                            CompletionCallback<Void> callback = invocation.getArgument(2);
                            callback.onCompleted(null);
                            return null;
                        })
                .when(mockStoreBackupTaskSender)
                .verifyStoreBackup(anyInt(), any(), any());

        BackupManager backupManager = new BackupManager(
                configs, mockMetaService, mockMetaStore, mockSnapshotManager, mockSchemaManager, mockSnapshotCache,
                mockStoreBackupTaskSender);
        backupManager.start();
        verify(mockSnapshotManager).addListener(any());

        // create the first backup
        CountDownLatch updateBackupIdLatch1 = new CountDownLatch(1);
        CountDownLatch updateBackupInfoByCreation1Latch = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            updateBackupIdLatch1.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(GLOBAL_BACKUP_ID_PATH, objectMapper.writeValueAsBytes(1));
        doAnswer(
                        invocation -> {
                            byte[] backupInfoBytes = invocation.getArgument(1);
                            List<BackupInfo> backupInfoList = objectMapper.readValue(backupInfoBytes,
                                    new TypeReference<List<BackupInfo>>() {});
                            assertEquals(backupInfoList.size(), 1);
                            assertEquals(backupInfoList.get(0), backupInfo1);
                            updateBackupInfoByCreation1Latch.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(BACKUP_INFO_PATH, objectMapper.writeValueAsBytes(Collections.singletonList(backupInfo1)));
        int backupId1 = backupManager.createNewBackup();
        assertEquals(backupId1, 1);
        assertTrue(updateBackupIdLatch1.await(5L, TimeUnit.SECONDS));
        assertTrue(updateBackupInfoByCreation1Latch.await(5L, TimeUnit.SECONDS));

        // create the second backup
        CountDownLatch updateBackupIdLatch2 = new CountDownLatch(1);
        CountDownLatch updateBackupInfoByCreation2Latch = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            updateBackupIdLatch2.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(GLOBAL_BACKUP_ID_PATH, objectMapper.writeValueAsBytes(2));
        doAnswer(
                        invocation -> {
                            byte[] backupInfoBytes = invocation.getArgument(1);
                            List<BackupInfo> backupInfoList = objectMapper.readValue(backupInfoBytes,
                                    new TypeReference<List<BackupInfo>>() {});
                            backupInfoList.sort(new Comparator<BackupInfo>() {
                                @Override
                                public int compare(BackupInfo o1, BackupInfo o2) {
                                    return o1.getGlobalBackupId() - o2.getGlobalBackupId();
                                }
                            });
                            assertEquals(backupInfoList.size(), 2);
                            assertEquals(backupInfoList.get(0), backupInfo1);
                            assertEquals(backupInfoList.get(1), backupInfo2);
                            updateBackupInfoByCreation2Latch.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(BACKUP_INFO_PATH, objectMapper.writeValueAsBytes(Arrays.asList(backupInfo1, backupInfo2)));
        int backupId2 = backupManager.createNewBackup();
        assertEquals(backupId2, 2);
        assertTrue(updateBackupIdLatch2.await(5L, TimeUnit.SECONDS));
        assertTrue(updateBackupInfoByCreation2Latch.await(5L, TimeUnit.SECONDS));

        // get backup info list and check
        assertEquals(backupManager.getBackupInfoList().size(), 2);

        // verify backups
        try {
            backupManager.verifyBackup(backupId1);
            backupManager.verifyBackup(backupId2);
        } catch (Exception e) {
            fail("should not have thrown any exception during backup verification");
        }

        // restore from the second backup
        try {
            backupManager.restoreFromBackup(backupId2, "restore_meta", "restore_store");
            assertTrue(Files.exists(Paths.get("restore_meta", "query_snapshot_id")));
            assertTrue(Files.exists(Paths.get("restore_meta", "graph_def_proto_bytes")));
            assertTrue(Files.exists(Paths.get("restore_meta", "queue_offsets")));
        } catch (Exception e) {
            fail("should not have thrown any exception during backup restoring");
        } finally {
            FileUtils.deleteDirectory(new File("restore_meta"));
        }

        // purge 1 old backup
        CountDownLatch updateBackupInfoByPurgingLatch = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            byte[] backupInfoBytes = invocation.getArgument(1);
                            List<BackupInfo> backupInfoList = objectMapper.readValue(backupInfoBytes,
                                    new TypeReference<List<BackupInfo>>() {});
                            assertEquals(backupInfoList.size(), 1);
                            assertEquals(backupInfoList.get(0), backupInfo2);
                            updateBackupInfoByPurgingLatch.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(BACKUP_INFO_PATH, objectMapper.writeValueAsBytes(Collections.singletonList(backupInfo2)));
        backupManager.purgeOldBackups(1);
        assertTrue(updateBackupInfoByPurgingLatch.await(5L, TimeUnit.SECONDS));

        // get backup info list and check
        assertEquals(backupManager.getBackupInfoList().size(), 1);

        // delete the remaining backup '2'
        CountDownLatch updateBackupInfoByDeletionLatch = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            byte[] backupInfoBytes = invocation.getArgument(1);
                            List<BackupInfo> backupInfoList = objectMapper.readValue(backupInfoBytes,
                                    new TypeReference<List<BackupInfo>>() {});
                            assertTrue(backupInfoList.isEmpty());
                            updateBackupInfoByDeletionLatch.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(BACKUP_INFO_PATH, objectMapper.writeValueAsBytes(new ArrayList<BackupInfo>()));
        backupManager.deleteBackup(2);
        assertTrue(updateBackupInfoByDeletionLatch.await(5L, TimeUnit.SECONDS));

        // get backup info list and check
        assertTrue(backupManager.getBackupInfoList().isEmpty());

        backupManager.stop();
        verify(mockSnapshotManager).removeListener(any());
    }
}
