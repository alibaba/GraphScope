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
package com.alibaba.maxgraph.tests.common.client;

import com.alibaba.maxgraph.tests.common.rpc.MockFactory;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.compiler.api.exception.NodeConnectException;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RoleClientsTest {

    @Test
    void testRoleClients() {
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.STORE_NODE_COUNT.getKey(), "1")
                        .put(CommonConfig.DISCOVERY_MODE.getKey(), "zookeeper")
                        .build();
        ChannelManager channelManager = new ChannelManager(configs, new MockFactory());
        RoleClients<MockRoleClient> clients =
                new RoleClients<>(channelManager, RoleType.STORE, MockRoleClient::new);
        channelManager.start();
        assertNotNull(clients.getClient(0));
        assertThrows(NodeConnectException.class, () -> clients.getClient(1));
        channelManager.stop();
    }

    class MockRoleClient extends RpcClient {

        public MockRoleClient(ManagedChannel channel) {
            super(channel);
        }
    }
}
