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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.ZkConfig;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphInitializer {

    private Configs configs;
    private CuratorFramework curator;
    private MetaStore metaStore;
    private LogService logService;
    private ObjectMapper objectMapper;

    public GraphInitializer(
            Configs configs, CuratorFramework curator, MetaStore metaStore, LogService logService) {
        this.configs = configs;
        this.curator = curator;
        this.metaStore = metaStore;
        this.logService = logService;

        this.objectMapper = new ObjectMapper();
    }

    public void initializeIfNeeded() {
        initializeLogServiceIfNeeded();
        try {
            initializeZkIfNeeded();
            initializeMetaIfNeeded();
        } catch (Exception e) {
            throw new MaxGraphException(e);
        }
    }

    private void initializeLogServiceIfNeeded() {
        if (!this.logService.initialized()) {
            this.logService.init();
        }
    }

    private void initializeZkIfNeeded() throws Exception {
        if (CommonConfig.DISCOVERY_MODE.get(this.configs).equalsIgnoreCase("zookeeper")) {
            String zkRoot = ZkConfig.ZK_BASE_PATH.get(configs);
            Stat stat = this.curator.checkExists().forPath(zkRoot);
            if (stat != null) {
                return;
            }
            this.curator.create().creatingParentsIfNeeded().forPath(zkRoot);
        }
    }

    private void initializeMetaIfNeeded() throws IOException {
        if (!this.metaStore.exists(SnapshotManager.QUERY_SNAPSHOT_INFO_PATH)) {
            SnapshotInfo snapshotInfo = new SnapshotInfo(-1L, -1L);
            byte[] b = this.objectMapper.writeValueAsBytes(snapshotInfo);
            this.metaStore.write(SnapshotManager.QUERY_SNAPSHOT_INFO_PATH, b);
        }
        if (!this.metaStore.exists(SnapshotManager.WRITE_SNAPSHOT_ID_PATH)) {
            byte[] b = this.objectMapper.writeValueAsBytes(0L);
            this.metaStore.write(SnapshotManager.WRITE_SNAPSHOT_ID_PATH, b);
        }
        if (!this.metaStore.exists(SnapshotManager.QUEUE_OFFSETS_PATH)) {
            int queueCount = CommonConfig.INGESTOR_QUEUE_COUNT.get(this.configs);
            List<Long> offsets = new ArrayList<>(queueCount);
            for (int i = 0; i < queueCount; i++) {
                offsets.add(-1L);
            }
            byte[] b = this.objectMapper.writeValueAsBytes(offsets);
            this.metaStore.write(SnapshotManager.QUEUE_OFFSETS_PATH, b);
        }
        if (!this.metaStore.exists(IdAllocator.ID_ALLOCATE_INFO_PATH)) {
            byte[] b = this.objectMapper.writeValueAsBytes(0L);
            this.metaStore.write(IdAllocator.ID_ALLOCATE_INFO_PATH, b);
        }
        if (!this.metaStore.exists(BackupManager.GLOBAL_BACKUP_ID_PATH)) {
            byte[] b = this.objectMapper.writeValueAsBytes(0);
            this.metaStore.write(BackupManager.GLOBAL_BACKUP_ID_PATH, b);
        }
        if (!this.metaStore.exists(BackupManager.BACKUP_INFO_PATH)) {
            List<BackupInfo> backupInfoList = new ArrayList<>();
            byte[] b = this.objectMapper.writeValueAsBytes(backupInfoList);
            this.metaStore.write(BackupManager.BACKUP_INFO_PATH, b);
        }
    }
}
