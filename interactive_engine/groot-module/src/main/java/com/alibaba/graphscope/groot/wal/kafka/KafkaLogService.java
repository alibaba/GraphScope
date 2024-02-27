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
package com.alibaba.graphscope.groot.wal.kafka;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.KafkaConfig;
import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaLogService implements LogService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLogService.class);

    private final Configs configs;
    private final String servers;
    private final String topic;
    private final int storeCount;
    private final short replicationFactor;
    private final int maxMessageMb;

    private volatile AdminClient adminClient;

    public KafkaLogService(Configs configs) {
        this.configs = configs;
        this.servers = KafkaConfig.KAFKA_SERVERS.get(configs);
        this.topic = KafkaConfig.KAFKA_TOPIC.get(configs);
        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.replicationFactor = KafkaConfig.KAFKA_REPLICATION_FACTOR.get(configs);
        this.maxMessageMb = KafkaConfig.KAFKA_MAX_MESSAGE_MB.get(configs);
        logger.info("Initialized KafkaLogService");
    }

    @Override
    public void init() {
        AdminClient admin = getAdmin();
        NewTopic newTopic = new NewTopic(this.topic, this.storeCount, this.replicationFactor);
        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", "-1");
        configs.put("retention.bytes", "-1");
        configs.put("max.message.bytes", String.valueOf(this.maxMessageMb * 1024 * 1024));
        newTopic.configs(configs);
        try {
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new GrootException("create topics [" + this.topic + "] failed", e);
        }
    }

    @Override
    public void destroy() {
        AdminClient admin = getAdmin();
        try {
            admin.deleteTopics(Arrays.asList(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new GrootException("delete topics [" + this.topic + "] failed", e);
        }
    }

    @Override
    public boolean initialized() {
        AdminClient admin = getAdmin();
        try {
            return admin.listTopics().names().get().contains(this.topic);
        } catch (InterruptedException | ExecutionException e) {
            throw new GrootException("list topics failed", e);
        }
    }

    @Override
    public LogWriter createWriter() {
        while (!initialized())
            ;
        String customConfigsStr = KafkaConfig.KAFKA_PRODUCER_CUSTOM_CONFIGS.get(configs);
        Map<String, String> customConfigs = new HashMap<>();
        if (!customConfigsStr.isEmpty()) {
            for (String item : customConfigsStr.split("\\|")) {
                String[] kv = item.split(":");
                if (kv.length != 2) {
                    throw new IllegalArgumentException(
                            "invalid kafka producer config: [" + item + "]");
                }
                customConfigs.put(kv[0], kv[1]);
            }
        }
        logger.info("Kafka writer configs {}", customConfigs);
        return new KafkaLogWriter(servers, topic, customConfigs);
    }

    @Override
    public LogReader createReader(int queueId, long offset) throws IOException {
        while (!initialized())
            ;
        return createReader(queueId, offset, -1);
    }

    public LogReader createReader(int queueId, long offset, long timestamp) throws IOException {
        return new KafkaLogReader(servers, getAdmin(), topic, queueId, offset, timestamp);
    }

    @Override
    public void deleteBeforeOffset(int queueId, long offset) throws IOException {
        AdminClient admin = getAdmin();
        TopicPartition partition = new TopicPartition(this.topic, queueId);
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(offset);
        Map<TopicPartition, RecordsToDelete> param = new HashMap<>();
        param.put(partition, recordsToDelete);
        DeleteRecordsResult result = admin.deleteRecords(param);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    private AdminClient getAdmin() {
        if (this.adminClient == null) {
            synchronized (this) {
                if (this.adminClient == null) {
                    try {
                        this.adminClient = createAdminWithRetry();
                    } catch (InterruptedException e) {
                        logger.error("Create Kafka Client interrupted");
                    }
                }
            }
        }
        logger.info("Created AdminClient");
        return this.adminClient;
    }

    private AdminClient createAdminWithRetry() throws InterruptedException {
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put("bootstrap.servers", this.servers);

        for (int i = 0; i < 30; ++i) {
            try {
                return AdminClient.create(adminConfig);
            } catch (Exception e) {
                logger.warn("Error creating Kafka AdminClient", e);
                Thread.sleep(10000);
            }
        }
        throw new RuntimeException("Create Kafka Client failed");
    }
}
