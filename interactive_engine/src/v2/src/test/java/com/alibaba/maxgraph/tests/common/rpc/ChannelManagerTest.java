package com.alibaba.maxgraph.tests.common.rpc;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.exception.NodeConnectException;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChannelManagerTest {

    @Test
    void testChannelManager() {
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.STORE_NODE_COUNT.getKey(), "1")
                .build();
        ChannelManager channelManager = new ChannelManager(configs, new MockFactory());
        channelManager.registerRole(RoleType.STORE);
        channelManager.start();
        Assertions.assertNotNull(channelManager.getChannel(RoleType.STORE, 0));
        Assertions.assertThrows(NodeConnectException.class, () -> channelManager.getChannel(RoleType.STORE, 1));
        channelManager.stop();
    }

}
