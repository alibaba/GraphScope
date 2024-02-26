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
package com.alibaba.graphscope.groot.servers;

import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.*;
import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.kafka.test.KafkaTestCluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MaxNode extends NodeBase {
    private static final Logger logger = LoggerFactory.getLogger(MaxNode.class);

    private KafkaTestCluster kafkaTestCluster;
    private final NodeBase coordinator;
    private final List<NodeBase> frontends = new ArrayList<>();
    private final List<NodeBase> stores = new ArrayList<>();

    public MaxNode(Configs configs) throws Exception {
        String zkConnectString = ZkConfig.ZK_CONNECT_STRING.get(configs);
        String kafkaServers = KafkaConfig.KAFKA_SERVERS.get(configs);
        if (CommonConfig.KAFKA_TEST_CLUSTER_ENABLE.get(configs)) {
            Properties kafkaConfigs = new Properties();
            kafkaConfigs.put("max.request.size", 10000000);
            this.kafkaTestCluster = new KafkaTestCluster(1, kafkaConfigs);
            this.kafkaTestCluster.start();
            zkConnectString = this.kafkaTestCluster.getZookeeperConnectString();
            kafkaServers = this.kafkaTestCluster.getKafkaConnectString();
        }

        int frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(configs);
        int storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);

        Configs baseConfigs =
                Configs.newBuilder(configs)
                        .put(ZkConfig.ZK_CONNECT_STRING.getKey(), zkConnectString)
                        .put(KafkaConfig.KAFKA_SERVERS.getKey(), kafkaServers)
                        .build();

        Configs coordinatorConfigs =
                Configs.newBuilder(baseConfigs)
                        .put(CommonConfig.ROLE_NAME.getKey(), RoleType.COORDINATOR.getName())
                        .put(CommonConfig.NODE_IDX.getKey(), "0")
                        .build();
        this.coordinator = new Coordinator(coordinatorConfigs);
        for (int i = 0; i < frontendCount; i++) {
            Configs frontendConfigs =
                    Configs.newBuilder(baseConfigs)
                            .put(CommonConfig.ROLE_NAME.getKey(), RoleType.FRONTEND.getName())
                            .put(CommonConfig.NODE_IDX.getKey(), String.valueOf(i))
                            .build();
            this.frontends.add(new Frontend(frontendConfigs));
        }
        for (int i = 0; i < storeCount; i++) {
            Configs storeConfigs =
                    Configs.newBuilder(baseConfigs)
                            .put(CommonConfig.ROLE_NAME.getKey(), RoleType.STORE.getName())
                            .put(CommonConfig.NODE_IDX.getKey(), String.valueOf(i))
                            .build();
            this.stores.add(new Store(storeConfigs));
        }
    }

    public void start() {
        List<Thread> startThreads = new ArrayList<>();
        startThreads.add(
                new Thread(
                        () -> {
                            this.coordinator.start();
                            logger.info("[" + this.coordinator.getName() + "] started");
                        }));
        for (NodeBase store : this.stores) {
            startThreads.add(
                    new Thread(
                            () -> {
                                store.start();
                                logger.info("[" + store.getName() + "] started");
                            }));
        }

        for (NodeBase frontend : this.frontends) {
            startThreads.add(
                    new Thread(
                            () -> {
                                frontend.start();
                                logger.info("[" + frontend.getName() + "] started");
                            }));
        }

        for (Thread startThread : startThreads) {
            startThread.start();
        }
        for (Thread startThread : startThreads) {
            try {
                startThread.join();
            } catch (InterruptedException e) {
                throw new GrootException(e);
            }
        }
        logger.info("MaxNode started");
    }

    @Override
    public void close() throws IOException {
        for (NodeBase frontend : this.frontends) {
            frontend.close();
        }
        for (NodeBase store : this.stores) {
            store.close();
        }
        this.coordinator.close();

        try {
            if (this.kafkaTestCluster != null) {
                this.kafkaTestCluster.close();
            }
        } catch (Exception e) {
            logger.warn("close kafka failed", e);
        }
    }

    public static void main(String[] args) throws Exception {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        MaxNode maxNode = new MaxNode(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(maxNode);
        nodeLauncher.start();
    }

    @VisibleForTesting
    public List<NodeBase> getStores() {
        return stores;
    }

    @VisibleForTesting
    public List<NodeBase> getFrontends() {
        return frontends;
    }
}
