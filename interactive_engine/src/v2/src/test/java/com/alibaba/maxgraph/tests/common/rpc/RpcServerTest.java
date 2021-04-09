package com.alibaba.maxgraph.tests.common.rpc;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.RpcServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RpcServerTest {

    @Test
    void testRpcServer() throws IOException {
        int port = 1111;
        Configs configs = Configs.newBuilder()
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
