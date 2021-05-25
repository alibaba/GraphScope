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
package com.alibaba.maxgraph.tests.store.executor;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.ZkConfig;
import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.discovery.ZkDiscovery;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.store.executor.ExecutorDiscoveryManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

public class ExecutorDiscoveryManagerTest {
    @Test
    void testExecutorServerStart() throws Exception {
        try (TestingServer testingServer = new TestingServer(-1)) {
            int zkPort = testingServer.getPort();
            Configs configs = Configs.newBuilder()
                    .put(ZkConfig.ZK_CONNECT_STRING.getKey(), "localhost:" + zkPort)
                    .put(ZkConfig.ZK_BASE_PATH.getKey(), "test_discovery")
                    .build();
            CuratorFramework curator = CuratorUtils.makeCurator(configs);
            curator.start();

            LocalNodeProvider engineServerProvider = new LocalNodeProvider(RoleType.EXECUTOR_ENGINE, configs);
            NodeDiscovery engineServerDiscovery = new ZkDiscovery(configs, engineServerProvider, curator);
            LocalNodeProvider storeQueryProvider = new LocalNodeProvider(RoleType.EXECUTOR_GRAPH, configs);
            NodeDiscovery storeQueryDiscovery = new ZkDiscovery(configs, storeQueryProvider, curator);
            LocalNodeProvider queryExecuteProvider = new LocalNodeProvider(RoleType.EXECUTOR_QUERY, configs);
            NodeDiscovery queryExecuteDiscovery = new ZkDiscovery(configs, queryExecuteProvider, curator);
            LocalNodeProvider queryManageProvider = new LocalNodeProvider(RoleType.EXECUTOR_MANAGE, configs);
            NodeDiscovery queryManageDiscovery = new ZkDiscovery(configs, queryManageProvider, curator);
            ExecutorDiscoveryManager executorDiscoveryManager = new ExecutorDiscoveryManager(engineServerProvider,
                    engineServerDiscovery,
                    storeQueryProvider,
                    storeQueryDiscovery,
                    queryExecuteProvider,
                    queryExecuteDiscovery,
                    queryManageProvider,
                    queryManageDiscovery);

            executorDiscoveryManager.getEngineServerProvider().apply(123);
            executorDiscoveryManager.getEngineServerDiscovery().start();

            executorDiscoveryManager.getQueryExecuteProvider().apply(1234);
            executorDiscoveryManager.getQueryExecuteDiscovery().start();
        }
    }
}
