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
package com.alibaba.maxgraph.common.zookeeper;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created on 16/11/30.
 *
 * @author beimian.mcq
 */
public class ZkConnectFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ZkConnectFactory.class);
    private static volatile ZkClient zkClient = null;
    private static String zkUrl;

    private static final Object LOCK = new Object();

    /**
     * Create or reuse a zkClient instance of {@link CuratorFramework};
     * Although the zkClient has called {@link CuratorFramework#start()}, but it may not connected to zk server,
     * you can call {@link CuratorFramework#blockUntilConnected(int, TimeUnit)} if needed;
     * Whenever you do not need the zkClient any more, you should call the {@link ZkClient#close()};
     *
     * @return : a {@link CuratorFramework} instance which is started, but may not connected.
     */
    static CuratorFramework getZkClient(final InstanceConfig instanceConfig) {

        Preconditions.checkArgument(zkUrl == null || (zkUrl != null && zkUrl.equals(instanceConfig.getZkConnect())), "zkUrl " +
                "change from " + zkUrl + " to " + instanceConfig.getZkConnect());

        if (zkClient == null || zkClient.isClosed()) {
            synchronized (LOCK) {
                if (zkClient == null || zkClient.isClosed()) {
                    zkClient = new ZkClient(instanceConfig);
                    zkClient.start();
                    zkUrl = instanceConfig.getZkConnect();
                    LOG.info("Create zk connect {} ...", zkUrl);
                }
            }
        }

        zkClient.increaseRef();
        return zkClient;
    }

    public static void forceClose() {
        if (zkClient != null) {
            synchronized (LOCK) {
                if (zkClient != null) {
                    LOG.info("Close zk connect : {}", zkUrl);
                    zkClient.forceClose();
                    zkClient = null;
                    zkUrl = null;
                }
            }
        }
    }
}
