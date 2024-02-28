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
package com.alibaba.graphscope.groot.meta;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.KafkaConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultMetaService implements MetaService {

    private final int partitionCount;
    private final int queueCount;
    private Map<Integer, List<Integer>> storeToPartitionIds;
    private Map<Integer, Integer> partitionToStore;
    private final int storeCount;
    private final String kafkaServers;
    private final String kafkaTopicName;

    public DefaultMetaService(Configs configs) {
        this.partitionCount = CommonConfig.PARTITION_COUNT.get(configs);
        this.queueCount = 1;
        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.kafkaServers = KafkaConfig.KAFKA_SERVERS.get(configs);
        this.kafkaTopicName = KafkaConfig.KAFKA_TOPIC.get(configs);
    }

    @Override
    public void start() {
        loadPartitions();
    }

    /**
     * Partitions assignment example:
     *
     * <p>| storeID | Partitions | | 0 | 0, 1, 2 | | 1 | 3, 4, 5 | | 2 | 6, 7 | | 3 | 8, 9 |
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
            for (int pid = startPartitionId; pid < nextStartPartitionId; pid++) {
                partitionIds.add(pid);
                this.partitionToStore.put(pid, i);
            }
            this.storeToPartitionIds.put(i, partitionIds);
        }
    }

    private static int getStartPartition(int avg, int storeId, int remainder) {
        return avg * storeId + Math.min(storeId, remainder);
    }

    @Override
    public void stop() {}

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
    public int getStoreCount() {
        return this.storeCount;
    }

    @Override
    public String getLoggerServers() {
        return this.kafkaServers;
    }

    @Override
    public String getLoggerTopicName() {
        return this.kafkaTopicName;
    }
}
