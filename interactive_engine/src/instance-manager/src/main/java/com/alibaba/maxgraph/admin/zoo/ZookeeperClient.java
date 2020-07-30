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
package com.alibaba.maxgraph.admin.zoo;

import com.alibaba.maxgraph.admin.config.InstanceProperties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClient implements AutoCloseable {
    private static ZookeeperClient zookeeperClient;
    private CuratorFramework client;

    public ZookeeperClient(String hosts,
                           int sessionMillSec,
                           int connectMillSec,
                           String namespace) {
        this.client = CuratorFrameworkFactory.builder()
                .connectString(hosts)
                .sessionTimeoutMs(sessionMillSec)
                .connectionTimeoutMs(connectMillSec)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace(namespace)
                .build();
    }

    public static synchronized ZookeeperClient getClient(InstanceProperties instanceProperties) {
        if (null == zookeeperClient) {
            zookeeperClient = new ZookeeperClient(instanceProperties.getZookeeper().getHosts(),
                    instanceProperties.getZookeeper().getSessionTimeoutMill(),
                    instanceProperties.getZookeeper().getConnectTimeoutMill(),
                    instanceProperties.getZookeeper().getNamespace());
            zookeeperClient.start();
        }
        return zookeeperClient;
    }

    public void start() {
        this.client.start();
    }

    public PathStatValue getValue(String path) throws Exception {
        Stat stat = new Stat();
        return new PathStatValue(new String(this.client.getData().storingStatIn(stat).forPath(path)), stat);
    }

    @Override
    public void close() throws Exception {
        if (null != this.client) {
            this.client.close();
            this.client = null;
        }
    }
}
