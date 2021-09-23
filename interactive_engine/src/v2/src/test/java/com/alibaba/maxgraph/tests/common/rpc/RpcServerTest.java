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
import com.alibaba.graphscope.groot.discovery.LocalNodeProvider;
import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.rpc.RpcServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RpcServerTest {

    @Test
    void testRpcServer() throws IOException {
        int port = 1111;
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.RPC_PORT.getKey(), String.valueOf(port))
                        .put(CommonConfig.ROLE_NAME.getKey(), RoleType.STORE.getName())
                        .put(CommonConfig.NODE_IDX.getKey(), "0")
                        .build();
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        RpcServer rpcServer = new RpcServer(configs, localNodeProvider);
        rpcServer.start();
        MaxGraphNode localNode = localNodeProvider.get();
        assertEquals(port, localNode.getPort());
        assertEquals(port, rpcServer.getPort());
        rpcServer.stop();
    }
}
