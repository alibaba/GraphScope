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
package com.alibaba.graphscope.groot.common.config;

import com.alibaba.graphscope.groot.common.meta.ServerDiscoverMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ZkConfig {
    public static final Config<String> ZK_BASE_PATH =
            Config.stringConfig("zk.base.path", "/graphscope/default_graph");

    public static final Config<String> ZK_CONNECT_STRING =
            Config.stringConfig("zk.connect.string", "localhost:2181/default_graph");

    public static final Config<Integer> ZK_CONNECTION_TIMEOUT_MS =
            Config.intConfig("zk.connection.timeout.ms", 1000);

    public static final Config<Integer> ZK_SESSION_TIMEOUT_MS =
            Config.intConfig("zk.session.timeout.ms", 30000);

    public static final Config<Integer> ZK_BASE_SLEEP_MS =
            Config.intConfig("zk.base.sleep.ms", 10000);

    public static final Config<Integer> ZK_MAX_SLEEP_MS =
            Config.intConfig("zk.max.sleep.ms", 60000);

    public static final Config<Integer> ZK_MAX_RETRY = Config.intConfig("zk.max.retry", 29);

    public static final Config<Boolean> ZK_AUTH_ENABLE = Config.boolConfig("zk.auth.enable", false);

    public static final Config<String> ZK_AUTH_USER = Config.stringConfig("zk.auth.user", "");

    public static final Config<String> ZK_AUTH_PASSWORD =
            Config.stringConfig("zk.auth.password", "");

    private static volatile String cachedZkServers;
    private static volatile long lastUpdateTime = 0L;

    public static String getZkServers(Configs configs) {
        String kafkaServerDiscoveryMode = CommonConfig.SERVERS_DISCOVERY_MODE.get(configs);
        if (kafkaServerDiscoveryMode == null) {
            return ZK_CONNECT_STRING.get(configs);
        }
        ServerDiscoverMode discoverMode = ServerDiscoverMode.fromMode(kafkaServerDiscoveryMode);
        switch (discoverMode) {
            case SERVICE:
                return ZK_CONNECT_STRING.get(configs);
            case FILE:
                String filePath = ZK_CONNECT_STRING.get(configs);
                Integer refreshIntervalMs = CommonConfig.FILE_DISCOVERY_INTERVAL_MS.get(configs);
                long now = System.currentTimeMillis();
                if (cachedZkServers == null || (now - lastUpdateTime) > refreshIntervalMs) {
                    synchronized (KafkaConfig.class) {
                        if (cachedZkServers == null || (now - lastUpdateTime) > refreshIntervalMs) {
                            try {
                                Path path = Paths.get(filePath);
                                if (Files.exists(path)) {
                                    cachedZkServers = Files.readString(path).trim();
                                    lastUpdateTime = now;
                                } else {
                                    throw new IllegalArgumentException("Zk servers file not found: " + filePath);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException("read Zk servers file error: " + filePath, e);
                            }
                        }
                    }
                }
                return cachedZkServers;
            default:
                return ZK_CONNECT_STRING.get(configs);
        }
    }

    public static void setZkServersToFile(String zkAddress, Configs configs) {
        String filePath = ZK_CONNECT_STRING.get(configs);
        Path path = Paths.get(filePath);
        try {
            Files.writeString(path, zkAddress);
            cachedZkServers = zkAddress.trim();
            lastUpdateTime = System.currentTimeMillis();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Zk servers file: " + filePath, e);
        }
    }

}
