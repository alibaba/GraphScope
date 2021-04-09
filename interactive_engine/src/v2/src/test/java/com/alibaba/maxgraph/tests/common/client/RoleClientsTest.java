package com.alibaba.maxgraph.tests.common.client;

import com.alibaba.maxgraph.tests.common.rpc.MockFactory;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.exception.NodeConnectException;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RoleClientsTest {

    @Test
    void testRoleClients() {
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.STORE_NODE_COUNT.getKey(), "1")
                .build();
        ChannelManager channelManager = new ChannelManager(configs, new MockFactory());
        RoleClients<MockRoleClient> clients = new RoleClients<>(channelManager, RoleType.STORE, MockRoleClient::new);
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
