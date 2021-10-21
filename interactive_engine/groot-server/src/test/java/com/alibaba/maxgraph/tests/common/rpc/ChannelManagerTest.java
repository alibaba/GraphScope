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
package com.alibaba.maxgraph.tests.common.rpc;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.compiler.api.exception.NodeConnectException;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChannelManagerTest {

    @Test
    void testChannelManager() {
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.STORE_NODE_COUNT.getKey(), "1")
                        .put(CommonConfig.DISCOVERY_MODE.getKey(), "zookeeper")
                        .build();
        ChannelManager channelManager = new ChannelManager(configs, new MockFactory());
        channelManager.registerRole(RoleType.STORE);
        channelManager.start();
        Assertions.assertNotNull(channelManager.getChannel(RoleType.STORE, 0));
        Assertions.assertThrows(
                NodeConnectException.class, () -> channelManager.getChannel(RoleType.STORE, 1));
        channelManager.stop();
    }
}
