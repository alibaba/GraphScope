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
package com.alibaba.graphscope.groot.wal.kafka;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.KafkaConfig;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
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

    private Configs configs;
    private String servers;
    private String topic;
    private int queueCount;
    private short replicationFactor;

    private volatile AdminClient adminClient;

    public KafkaLogService(Configs configs) {
        this.configs = configs;
        this.servers = KafkaConfig.KAFKA_SERVERS.get(configs);
        this.topic = KafkaConfig.KAKFA_TOPIC.get(configs);
        this.queueCount = CommonConfig.INGESTOR_QUEUE_COUNT.get(configs);
        this.replicationFactor = KafkaConfig.KAFKA_REPLICATION_FACTOR.get(configs);
    }

    @Override
    public void init() {
        AdminClient admin = getAdmin();
        NewTopic newTopic = new NewTopic(this.topic, this.queueCount, this.replicationFactor);
        try {
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new MaxGraphException("create topics [" + this.topic + "] failed", e);
        }
    }

    @Override
    public void destroy() {
        AdminClient admin = getAdmin();
        try {
            admin.deleteTopics(Arrays.asList(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new MaxGraphException("delete topics [" + this.topic + "] failed", e);
        }
    }

    @Override
    public boolean initialized() {
        AdminClient admin = getAdmin();
        try {
            return admin.listTopics().names().get().contains(this.topic);
        } catch (InterruptedException | ExecutionException e) {
            throw new MaxGraphException("list topics failed", e);
        }
    }

    @Override
    public LogWriter createWriter(int queueId) {
        String customConfigsStr = KafkaConfig.KAFKA_PRODUCER_CUSTOM_CONFIGS.get(configs);
        Map<String, String> customConfigs = new HashMap<>();
        if (!"".equals(customConfigsStr)) {
            for (String item : customConfigsStr.split("\\|")) {
                String[] kv = item.split(":");
                if (kv.length != 2) {
                    throw new IllegalArgumentException("invalid kafka producer config: [" + item + "]");
                }
                customConfigs.put(kv[0], kv[1]);
            }
        }
        return new KafkaLogWriter(servers, topic, queueId, customConfigs);
    }

    @Override
    public LogReader createReader(int queueId, long offset) throws IOException {
        return new KafkaLogReader(servers, getAdmin(), topic, queueId, offset);
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
                    Map<String, Object> adminConfig = new HashMap<>();
                    adminConfig.put("bootstrap.servers", this.servers);
                    this.adminClient = AdminClient.create(adminConfig);
                }
            }
        }
        return this.adminClient;
    }
}
