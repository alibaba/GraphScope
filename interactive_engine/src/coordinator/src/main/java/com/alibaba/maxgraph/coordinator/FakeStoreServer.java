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
package com.alibaba.maxgraph.coordinator;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.server.AbstractRpcServer;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.common.util.RpcUtils;
import com.alibaba.maxgraph.coordinator.client.ServerDataApiClient;
import com.alibaba.maxgraph.proto.*;
import com.google.common.collect.Lists;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-20 上午10:31
 **/

public class FakeStoreServer extends AbstractRpcServer {

    private int serverId;
    private String hostName;
    private InstanceConfig instanceConfig;
    private Server rpcServer;
    private Endpoint endpoint;
    private ServerDataApiClient serverDataApiClient;
    private final ScheduledExecutorService taskSchedulerService;
    private Integer messageCount = 0;

    private static final Logger LOG = LoggerFactory.getLogger(com.alibaba.maxgraph.coordinator.FakeStoreServer.class);

    public FakeStoreServer(InstanceConfig instanceConfig, int fakeServerId) {
        this.instanceConfig = instanceConfig;
        this.serverId = fakeServerId;
        this.serverDataApiClient = new ServerDataApiClient(instanceConfig);
        this.taskSchedulerService = new ScheduledThreadPoolExecutor(1,
                CommonUtil.createFactoryWithDefaultExceptionHandler("fake_server", LOG));
    }

    public void start() throws Exception {
        startRpcServer();

        this.serverDataApiClient.start();

        taskSchedulerService.scheduleWithFixedDelay(() -> {
            try {
                heartbeat();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void startRpcServer() throws IOException {
        this.hostName = InetAddress.getLocalHost().getHostAddress();
        NettyServerBuilder serverBuilder = NettyServerBuilder
                .forAddress(new InetSocketAddress(hostName, 0))
                .maxMessageSize(Integer.MAX_VALUE);

        this.init(serverBuilder);
        this.rpcServer = serverBuilder.build().start();
        this.endpoint = new Endpoint(hostName, rpcServer.getPort());
    }

    public void heartbeat() throws Exception {
        serverDataApiClient.storeHb2Coordinator(serverId, endpoint);
    }

    public Integer getMessageCount() {
        return messageCount;
    }

    @Override
    public BindableService getService() {
        return null;
    }


    public static void main(String[] args) throws Exception {
        InstanceConfig instanceConfig = CommonUtil.getInstanceConfig(args, 1);

        FakeStoreServer fakeServer = new FakeStoreServer(instanceConfig, 1);
        fakeServer.start();

        CountDownLatch shutdown = new CountDownLatch(1);
        fakeServer.waitShutdown(shutdown);
    }

    public void waitShutdown(CountDownLatch shutdownSingle) throws IOException {
        while (true) {
            try {
                shutdownSingle.await();
                shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
    }
}
