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

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.zookeeper.ZkConnectFactory;
import com.alibaba.maxgraph.common.zookeeper.ZkNamingProxy;
import com.alibaba.maxgraph.common.zookeeper.ZkUtils;
import com.alibaba.maxgraph.coordinator.client.ServerDataApiClient;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Setup a cluster in a single jvm process, usually for testing.
 * <p>
 * Created by xiafei.qiuxf on 16/8/11.
 */
public class TestingCluster {

    private static final Logger LOG = LoggerFactory.getLogger(TestingCluster.class);

    private final TestingServer zkServer;
    private final ZkUtils zkUtils;
    private final ZkNamingProxy storeZKUtil;
    private final String clusterName;
    private final InstanceConfig conf;
    private final int zkPort;
    private ServerDataApiClient ctlCli;
    private Coordinator coordinator;
    private List<FakeStoreServer> servers = Lists.newArrayList();

    public TestingCluster(String clusterName, InstanceConfig userConf, String zkPrefix) throws Exception {
        ZkConnectFactory.forceClose();
        this.clusterName = clusterName;
        // start zk test server
        zkServer = new TestingServer(-1);
        zkServer.start();
        zkPort = zkServer.getPort();
        LOG.debug("zk server of testing cluster [{}] listening on {}", clusterName, zkPort);
        if (!StringUtils.isEmpty(zkPrefix)) {
            CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                    .connectString(String.format("0.0.0.0:%d/", zkPort))
                    .connectionTimeoutMs(26000)
                    .sessionTimeoutMs(26000)
                    .retryPolicy(new ExponentialBackoffRetry(500, 3))
                    .build();
            zkClient.start();
            zkClient.create().creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath("/" + zkPrefix);
            zkClient.close();
        }

        this.conf = userConf;
        conf.set(InstanceConfig.ZK_CONNECT, String.format("0.0.0.0:%d/%s", zkPort, zkPrefix));
        conf.set(InstanceConfig.ZK_SESSION_TIMEOUT_MS, 10000);
        conf.set(InstanceConfig.ZK_CONNECTION_TIMEOUT_MS, 3000);
        userConf.getAll().forEach(conf::set);

        zkUtils = new ZkUtils(conf.getZkConnect(), conf.getZkSessionTimeoutMs(), conf.getZkConnectionTimeoutMs());
        zkUtils.getZkClient().blockUntilConnected();

        this.storeZKUtil = new ZkNamingProxy(conf.getGraphName(), zkUtils);
    }

    public TestingCluster(String clusterName, int serverNum, InstanceConfig userConf) throws Exception {
        this(clusterName, serverNum, "", userConf);
    }

    public TestingCluster(String clusterName, int serverNum, String zkPrefix, InstanceConfig userConf) throws Exception {
       this(clusterName, userConf, zkPrefix);

        addController();

        for (int i = 0; i < 150; i++) {
            TimeUnit.MILLISECONDS.sleep(1000);
            Endpoint endpoint = storeZKUtil.getCoordinatorEndpoint();
            if (endpoint == null) {
                LOG.debug("waiting for coordinator...");
            } else {
                ctlCli = new ServerDataApiClient(conf);
                ctlCli.start();
                return;
            }
        }
        throw new RuntimeException("controller not found");
    }

    private void addController() throws Exception {
        conf.set(InstanceConfig.SERVER_ID, 0);
        this.coordinator = new Coordinator(conf);
        coordinator.start();
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public void addServer(FakeStoreServer fakeServer) {
        this.servers.add(fakeServer);
    }

    public int getZkPort() {
        return zkPort;
    }

    public void shutdownServer(int i) throws Exception {
        this.servers.get(i).shutdown();
    }

    public ServerDataApiClient getCtlCli() {
        return ctlCli;
    }

    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    public ZkNamingProxy getStoreZKUtil() {
        return storeZKUtil;
    }

    public InstanceConfig getConf() {
        return conf;
    }

    public void waitServersReady() {
        // TODO wait server ready
    }

    public void shutdown() throws Exception {
        storeZKUtil.getZkUtils().getZkClient().close();

        if (ctlCli != null) {
            ctlCli.close();
        }

        // TODO shutdown server

        if (coordinator != null) {
            coordinator.shutdown();
        }
        ZkConnectFactory.forceClose();
        zkServer.close();
        FileUtils.deleteQuietly(new File("./" + clusterName));
    }


    public void restart() throws Exception {
        int serverNum = servers.size();
        for (int id = 0; id < serverNum; id++) {
            this.shutdownServer(id);
        }
        ZkConnectFactory.forceClose();

        LOG.info("Cluster restarting ... ...");
        TimeUnit.SECONDS.sleep(2);
    }

    public static TestingCluster create(String graphName) throws Exception {
        HashMap<String, String> params = Maps.newHashMap();
        params.put(InstanceConfig.GRAPH_NAME, graphName);
        params.put(InstanceConfig.VPC_ENDPOINT, graphName);
        params.put(InstanceConfig.RESOURCE_EXECUTOR_COUNT, "2");
        params.put(InstanceConfig.PARTITION_NUM, "4");
        params.put(InstanceConfig.DATA_DECIDER_THREAD_INTERVAL_SECONDS, "1");
        params.put(InstanceConfig.SERVER_ID, "0");
        params.put(InstanceConfig.STORE_ALLOW_MEMORY, "true");

        return new TestingCluster("test", new InstanceConfig(params), "");
    }
}
