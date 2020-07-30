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
package com.alibaba.maxgraph.coordinator.client;

import com.alibaba.maxgraph.common.client.BaseAutoRefreshClient;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.zookeeper.ZkNamingProxy;
import com.alibaba.maxgraph.common.zookeeper.ZkUtils;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-15 上午11:05
 **/

public abstract class BaseCoordinatorClient<T> extends BaseAutoRefreshClient {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCoordinatorClient.class);

    protected final String graphName;
    protected ZkUtils zkUtils;
    protected ZkNamingProxy namingProxy;

    protected final AtomicReference<T> serverStub = new AtomicReference<>();
    protected final AtomicReference<ManagedChannel> channel = new AtomicReference<>();

    public BaseCoordinatorClient(InstanceConfig instanceConfig) {
        super(instanceConfig.getGraphName(), instanceConfig.getClientRetryTimes(), 5);
        this.graphName = instanceConfig.getGraphName();
        this.zkUtils = new ZkUtils(instanceConfig.getZkConnect(), instanceConfig.getZkSessionTimeoutMs(),
                instanceConfig.getZkConnectionTimeoutMs(), 0, instanceConfig.getZkAuthEnable(), instanceConfig
                .getZkAuthUser(), instanceConfig.getZkAuthPassword());
        this.namingProxy = new ZkNamingProxy(graphName, zkUtils);
    }

    public BaseCoordinatorClient(InstanceConfig instanceConfig, ZkUtils zkUtils) {
        super(instanceConfig.getGraphName(), instanceConfig.getClientRetryTimes(), 5);
        this.graphName = instanceConfig.getGraphName();
        this.zkUtils = zkUtils;
        this.namingProxy = new ZkNamingProxy(graphName, zkUtils);
    }

    protected void closeChannel() {
        ManagedChannel managedChannel = channel.get();
        if (managedChannel != null && !managedChannel.isShutdown()) {
            try {
                managedChannel.shutdownNow().awaitTermination(3L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public ZkNamingProxy getZkNamingProxy() {
        return this.namingProxy;
    }

    @Override
    public void close() throws IOException {
        super.close();
        zkUtils.close();
        closeChannel();
    }
}
