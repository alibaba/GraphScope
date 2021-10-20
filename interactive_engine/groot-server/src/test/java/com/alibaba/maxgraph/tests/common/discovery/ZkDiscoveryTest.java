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
package com.alibaba.maxgraph.tests.common.discovery;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.ZkConfig;
import com.alibaba.graphscope.groot.discovery.LocalNodeProvider;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.discovery.ZkDiscovery;
import com.alibaba.maxgraph.common.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ZkDiscoveryTest {

    @Test
    void testDiscovery() throws Exception {
        try (TestingServer testingServer = new TestingServer(-1)) {
            int zkPort = testingServer.getPort();
            Configs zkConfigs =
                    Configs.newBuilder()
                            .put(ZkConfig.ZK_CONNECT_STRING.getKey(), "localhost:" + zkPort)
                            .put(ZkConfig.ZK_BASE_PATH.getKey(), "test_discovery")
                            .build();
            CuratorFramework curator = CuratorUtils.makeCurator(zkConfigs);
            curator.start();

            RoleType role = RoleType.STORE;
            NodeDiscovery discovery1 = createNodeDiscovery(role, 0, 1111, zkConfigs, curator);
            NodeDiscovery discovery2 = createNodeDiscovery(role, 1, 2222, zkConfigs, curator);

            discovery1.start();
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);
            NodeDiscovery.Listener mockListener = mock(NodeDiscovery.Listener.class);
            doAnswer(
                            invocationOnMock -> {
                                latch1.countDown();
                                return null;
                            })
                    .when(mockListener)
                    .nodesJoin(
                            RoleType.STORE, Collections.singletonMap(0, discovery1.getLocalNode()));
            doAnswer(
                            invocationOnMock -> {
                                latch2.countDown();
                                return null;
                            })
                    .when(mockListener)
                    .nodesJoin(
                            RoleType.STORE, Collections.singletonMap(1, discovery2.getLocalNode()));
            discovery1.addListener(mockListener);
            assertTrue(latch1.await(5L, TimeUnit.SECONDS));
            discovery2.start();
            assertTrue(latch2.await(5L, TimeUnit.SECONDS));

            verify(mockListener, times(2)).nodesJoin(any(), any());
            verify(mockListener, never()).nodesLeft(any(), any());

            discovery2.stop();
            verify(mockListener, timeout(5000L))
                    .nodesLeft(
                            RoleType.STORE, Collections.singletonMap(1, discovery2.getLocalNode()));

            discovery1.stop();
            curator.close();
        }
    }

    private NodeDiscovery createNodeDiscovery(
            RoleType role, int idx, int port, Configs zkConfigs, CuratorFramework curator) {
        Configs nodeConfigs =
                Configs.newBuilder()
                        .put(CommonConfig.ROLE_NAME.getKey(), role.getName())
                        .put(CommonConfig.NODE_IDX.getKey(), String.valueOf(idx))
                        .build();
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(nodeConfigs);
        localNodeProvider.apply(port);
        return new ZkDiscovery(zkConfigs, localNodeProvider, curator);
    }
}
