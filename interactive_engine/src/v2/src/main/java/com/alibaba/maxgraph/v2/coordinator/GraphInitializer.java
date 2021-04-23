package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.ZkConfig;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.wal.LogService;
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

    public GraphInitializer(Configs configs, CuratorFramework curator, MetaStore metaStore, LogService logService) {
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
        String zkRoot = ZkConfig.ZK_BASE_PATH.get(configs);
        Stat stat = this.curator.checkExists().forPath(zkRoot);
        if (stat != null) {
            return;
        }
        this.curator.create().creatingParentsIfNeeded().forPath(zkRoot);
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
    }

}
