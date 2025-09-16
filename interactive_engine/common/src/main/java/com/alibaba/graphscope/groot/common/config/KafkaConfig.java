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
package com.alibaba.graphscope.groot.common.config;

import com.alibaba.graphscope.groot.common.meta.ServerDiscoverMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KafkaConfig {
    public static final Config<String> KAFKA_SERVERS =
            Config.stringConfig("kafka.servers", "localhost:9092");

    public static final Config<String> KAFKA_TOPIC =
            Config.stringConfig("kafka.topic", "graphscope");

    public static final Config<Short> KAFKA_REPLICATION_FACTOR =
            Config.shortConfig("kafka.replication.factor", (short) 1);

    public static final Config<String> KAFKA_PRODUCER_CUSTOM_CONFIGS =
            Config.stringConfig("kafka.producer.custom.configs", "");

    public static final Config<Integer> KAFKA_MAX_MESSAGE_MB =
            Config.intConfig("kafka.max.message.mb", 20);

    private static volatile String cachedKafkaServers;
    private static volatile long lastUpdateTime = 0L;

    public static String getKafkaServers(Configs configs) {
        String kafkaServerDiscoveryMode = CommonConfig.SERVERS_DISCOVERY_MODE.get(configs);
        if (kafkaServerDiscoveryMode == null) {
            return KAFKA_SERVERS.get(configs);
        }
        ServerDiscoverMode discoverMode = ServerDiscoverMode.fromMode(kafkaServerDiscoveryMode);
        switch (discoverMode) {
            case SERVICE:
                return KAFKA_SERVERS.get(configs);
            case FILE:
                String filePath = KAFKA_SERVERS.get(configs);
                Integer refreshIntervalMs = CommonConfig.FILE_DISCOVERY_INTERVAL_MS.get(configs);
                long now = System.currentTimeMillis();
                if (cachedKafkaServers == null || (now - lastUpdateTime) > refreshIntervalMs) {
                    synchronized (KafkaConfig.class) {
                        if (cachedKafkaServers == null || (now - lastUpdateTime) > refreshIntervalMs) {
                            try {
                                Path path = Paths.get(filePath);
                                if (Files.exists(path)) {
                                    cachedKafkaServers = Files.readString(path).trim();
                                    lastUpdateTime = now;
                                } else {
                                    throw new IllegalArgumentException("Kafka servers file not found: " + filePath);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException("read Kafka ip file error: " + filePath, e);
                            }
                        }
                    }
                }
                return cachedKafkaServers;
            default:
                return KAFKA_SERVERS.get(configs);
        }
    }

    public static void setKafkaServersToFile(String kafkaAddress, Configs configs) {
        String filePath = KAFKA_SERVERS.get(configs);
        Path path = Paths.get(filePath);
        synchronized (KafkaConfig.class) {
            try {
                Files.writeString(path, kafkaAddress);
                cachedKafkaServers = kafkaAddress.trim();
                lastUpdateTime = System.currentTimeMillis();
            } catch (IOException e) {
                throw new RuntimeException("Failed to write Kafka servers file: " + filePath, e);
            }
        }
    }
}
