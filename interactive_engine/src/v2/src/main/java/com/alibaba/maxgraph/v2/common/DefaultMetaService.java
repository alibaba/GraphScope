package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultMetaService implements MetaService {

    private Configs configs;
    private int partitionCount;
    private int queueCount;
    private Map<Integer, List<Integer>> storeToPartitionIds;
    private Map<Integer, Integer> partitionToStore;
    private int storeCount;

    public DefaultMetaService(Configs configs) {
        this.configs = configs;
        this.partitionCount = CommonConfig.PARTITION_COUNT.get(configs);
        this.queueCount = CommonConfig.INGESTOR_QUEUE_COUNT.get(configs);
        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
    }

    @Override
    public void start() {
        loadPartitions();
    }

    /**
     * Partitions assignment example:
     *
     * | storeID |  Partitions |
     * | 0       | 0, 1, 2     |
     * | 1       | 3, 4, 5     |
     * | 2       | 6, 7        |
     * | 3       | 8, 9        |
     */
    private void loadPartitions() {
        this.storeToPartitionIds = new HashMap<>();
        this.partitionToStore = new HashMap<>();
        int avg = this.partitionCount / this.storeCount;
        int remainder = this.partitionCount % storeCount;
        for (int i = 0; i < storeCount; i++) {
            int startPartitionId = getStartPartition(avg, i, remainder);
            int nextStartPartitionId = getStartPartition(avg, i + 1, remainder);
            List<Integer> partitionIds = new ArrayList<>();
            for (int partitionId = startPartitionId; partitionId < nextStartPartitionId; partitionId++) {
                partitionIds.add(partitionId);
                this.partitionToStore.put(partitionId, i);
            }
            this.storeToPartitionIds.put(i, partitionIds);
        }
    }

    private static int getStartPartition(int avg, int storeId, int remainder) {
        return avg * storeId + Math.min(storeId, remainder);
    }

    @Override
    public void stop() {
    }

    @Override
    public int getPartitionCount() {
        return this.partitionCount;
    }

    @Override
    public int getStoreIdByPartition(int partitionId) {
        return this.partitionToStore.get(partitionId);
    }

    @Override
    public List<Integer> getPartitionsByStoreId(int storeId) {
        return this.storeToPartitionIds.get(storeId);
    }

    @Override
    public int getQueueCount() {
        return this.queueCount;
    }

    @Override
    public List<Integer> getQueueIdsForIngestor(int ingestorId) {
        return Arrays.asList(ingestorId);
    }

    @Override
    public int getIngestorIdForQueue(int queueId) {
        return queueId;
    }

    @Override
    public int getStoreCount() {
        return this.storeCount;
    }
}
