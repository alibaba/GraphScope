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
package com.alibaba.graphscope.groot.rpc;

import static com.alibaba.graphscope.groot.common.util.RpcUtils.createGrpcExecutor;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.discovery.GrootNode;
import com.alibaba.graphscope.groot.discovery.LocalNodeProvider;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RpcServer {
    public static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    protected final Server server;
    private LocalNodeProvider localNodeProvider;

    public RpcServer(
            Configs conf, LocalNodeProvider localNodeProvider, BindableService... services) {
        this(
                CommonConfig.RPC_PORT.get(conf),
                CommonConfig.RPC_THREAD_COUNT.get(conf),
                CommonConfig.RPC_MAX_BYTES_MB.get(conf) * 1024 * 1024,
                localNodeProvider,
                services);
    }

    public RpcServer(
            int port,
            int threadCount,
            int maxMessageSize,
            LocalNodeProvider localNodeProvider,
            BindableService... services) {
        this(
                NettyServerBuilder.forPort(port)
                        .executor(createGrpcExecutor(threadCount))
                        .maxInboundMessageSize(maxMessageSize),
                localNodeProvider,
                services);
    }

    public RpcServer(
            ServerBuilder<?> serverBuilder,
            LocalNodeProvider localNodeProvider,
            BindableService[] services) {
        for (BindableService service : services) {
            serverBuilder.addService(service);
        }
        this.server = serverBuilder.build();
        this.localNodeProvider = localNodeProvider;
    }

    public void start() throws IOException {
        server.start();
        GrootNode localNode = this.localNodeProvider.apply(server.getPort());
        logger.info("RpcServer started, node [" + localNode + "]");
    }

    public void stop() {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
        logger.info("RpcServer stopped");
    }

    public int getPort() {
        return this.server.getPort();
    }
}
