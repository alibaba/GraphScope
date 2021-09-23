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

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.CoordinatorConfig;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.coordinator.IngestorWriteSnapshotIdNotifier;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.coordinator.SnapshotInfo;
import com.alibaba.graphscope.groot.coordinator.SnapshotManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.alibaba.graphscope.groot.coordinator.SnapshotManager.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class SnapshotManagerTest {

    @Test
    void testSnapshotManager() throws IOException, InterruptedException {
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.INGESTOR_QUEUE_COUNT.getKey(), "1")
                        .put(CommonConfig.STORE_NODE_COUNT.getKey(), "2")
                        .put(CommonConfig.FRONTEND_NODE_COUNT.getKey(), "1")
                        .put(CommonConfig.INGESTOR_NODE_COUNT.getKey(), "1")
                        .put(CoordinatorConfig.SNAPSHOT_INCREASE_INTERVAL_MS.getKey(), "1000")
                        .put(CoordinatorConfig.OFFSETS_PERSIST_INTERVAL_MS.getKey(), "1000")
                        .build();

        long querySnapshotId = 10L;
        long writeSnapshotId = 12L;
        List<Long> queueOffsets = Arrays.asList(50L);
        long commitSnapshotId1 = 11L;
        long commitSnapshotId2 = 12L;
        List<Long> commitQueueOffsets1 = Arrays.asList(60L);
        List<Long> commitQueueOffsets2 = Arrays.asList(70L);

        ObjectMapper objectMapper = new ObjectMapper();

        MetaStore mockMetaStore = mock(MetaStore.class);
        when(mockMetaStore.exists(anyString())).thenReturn(true);
        when(mockMetaStore.read(QUERY_SNAPSHOT_INFO_PATH))
                .thenReturn(
                        objectMapper.writeValueAsBytes(
                                new SnapshotInfo(querySnapshotId, querySnapshotId)));
        when(mockMetaStore.read(WRITE_SNAPSHOT_ID_PATH))
                .thenReturn(objectMapper.writeValueAsBytes(writeSnapshotId));
        when(mockMetaStore.read(QUEUE_OFFSETS_PATH))
                .thenReturn(objectMapper.writeValueAsBytes(queueOffsets));

        CountDownLatch updateWriteSnapshotLatch = new CountDownLatch(1);
        doAnswer(
                        invocationOnMock -> {
                            updateWriteSnapshotLatch.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(WRITE_SNAPSHOT_ID_PATH, objectMapper.writeValueAsBytes(writeSnapshotId + 1));

        CountDownLatch updateQueueOffsetLatch = new CountDownLatch(1);
        doAnswer(
                        invocationOnMock -> {
                            updateQueueOffsetLatch.countDown();
                            return null;
                        })
                .when(mockMetaStore)
                .write(QUEUE_OFFSETS_PATH, objectMapper.writeValueAsBytes(commitQueueOffsets1));

        IngestorWriteSnapshotIdNotifier mockWriteSnapshotIdNotifier =
                mock(IngestorWriteSnapshotIdNotifier.class);

        CountDownLatch updateIngestorLatch = new CountDownLatch(1);
        doAnswer(
                        invocationOnMock -> {
                            updateIngestorLatch.countDown();
                            return null;
                        })
                .when(mockWriteSnapshotIdNotifier)
                .notifyWriteSnapshotIdChanged(writeSnapshotId + 1);

        LogService mockLogService = mock(LogService.class);
        SnapshotManager snapshotManager =
                new SnapshotManager(
                        configs, mockMetaStore, mockLogService, mockWriteSnapshotIdNotifier);
        snapshotManager.start();

        assertEquals(snapshotManager.getQueueOffsets(), queueOffsets);
        assertTrue(updateWriteSnapshotLatch.await(5L, TimeUnit.SECONDS));
        assertTrue(updateIngestorLatch.await(5L, TimeUnit.SECONDS));

        snapshotManager.commitSnapshotId(0, commitSnapshotId1, 10L, commitQueueOffsets1);
        snapshotManager.commitSnapshotId(1, commitSnapshotId2, 10L, commitQueueOffsets2);

        verify(mockMetaStore)
                .write(
                        QUERY_SNAPSHOT_INFO_PATH,
                        objectMapper.writeValueAsBytes(new SnapshotInfo(commitSnapshotId1, 10L)));

        assertTrue(updateQueueOffsetLatch.await(5L, TimeUnit.SECONDS));
        assertEquals(snapshotManager.getQueueOffsets(), commitQueueOffsets1);

        snapshotManager.stop();
    }
}
