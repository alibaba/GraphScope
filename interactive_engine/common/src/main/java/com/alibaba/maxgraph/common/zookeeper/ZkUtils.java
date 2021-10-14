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
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for all Zookeeper operations.
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-11 下午4:43
 **/

public class ZkUtils implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);
    private String zkUrl;
    private CuratorFramework zkClient;

    public ZkUtils(String zkUrl, int connectionTimeout, int sessionTimeout) {
        this(zkUrl, connectionTimeout, sessionTimeout, 0, false, null, null);
    }

    public ZkUtils(String zkUrl, int connectionTimeout, int sessionTimeout, int maxWaitMS, boolean zkAuthEnable,
                   String user, String password) {
        this.zkUrl= zkUrl;
        this.zkClient = new ZkClient(zkUrl, connectionTimeout, sessionTimeout, zkAuthEnable, user, password);
        this.zkClient.start();

        try {
            if (maxWaitMS > 0) {
                zkClient.blockUntilConnected(maxWaitMS, TimeUnit.MILLISECONDS);
            } else {
                this.zkClient.blockUntilConnected();
            }
        } catch (Exception e) {
            LOG.error("fail to create curator client: " + zkUrl, e);
            throw new RuntimeException(e);
        }
    }

    private ZkUtils(String zkUrl, CuratorFramework zkClient) {
        this.zkUrl = zkUrl;
        this.zkClient = zkClient;
    }

    /**
     * Singleton zk client for all zookeeper address
     */
    public static ZkUtils getZKUtils(InstanceConfig instanceConfig) {
        String zkUrl = instanceConfig.getZkConnect();
        try {
            CuratorFramework zkClient = ZkConnectFactory.getZkClient(instanceConfig);
            return new ZkUtils(zkUrl, zkClient);
        } catch (Exception e) {
            LOG.error("fail to create curator client: " +  zkUrl, e);
            throw new RuntimeException(e);
        }
    }

    public void setSessionExpireListener(AbstractRegisterCallBack callBack) {
        addConnectionStateListener(new SessionExpireListener(callBack));
    }

    public CuratorFramework getZkClient() {
        return zkClient;
    }

    // zk data operation
    public List<String> getPathChildren(String path) {
        try {
            return zkClient.getChildren().forPath(path);
        } catch (Exception e) {
            LOG.warn(path, e);
            return ImmutableList.of();
        }
    }

    public Pair<String, Stat> readDataMaybeNull(String path) {
        Stat stat = new Stat();
        try {
            String dataStr = new String(zkClient.getData().storingStatIn(stat).forPath(path));
            return ImmutablePair.of(dataStr, stat);
        } catch (KeeperException.NoNodeException e) {
            return ImmutablePair.of(null, stat);
        } catch (Exception e) {
            LOG.error("read data from zk error; ", e);
            return ImmutablePair.of(null, stat);
        }
    }

    public void deleteRecursive(String path) {
        try {
            zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            LOG.info(path + " deleted during connection loss; this is ok");
        }
    }

    public void makeSurePersistentPathExists(String path) {
        if (!pathExists(path)) {
            createPath(path);
        }
    }

    /**
     * Update the toInt of a persistent node with the given path and data.
     * create parent directory if necessary. Never throw NodeExistException.
     * Return the updated path zkVersion
     */
    public void updatePersistentPath(String path, String data) {
        try {
            createPersistentPath(path, data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean pathExists(String path) {
        try {
            return zkClient.checkExists().forPath(path) != null;
        } catch (KeeperException.NoNodeException e) {
            return false;
        } catch (Exception e) {
            LOG.error(path, e);
            throw new RuntimeException(e);
        }
    }

    public void deletePath(String path) {
        try {
            zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
        } catch (Exception e) {
            LOG.error(path, e);
        }
    }

    private void createPath(String path) {
        try {
            zkClient.create().creatingParentsIfNeeded().forPath(path, "".getBytes());
        } catch (KeeperException.NodeExistsException e) {
            if (!pathExists(path)) {
                throw new RuntimeException("can't create path: " + path, e);
            }
        } catch (Exception e) {
            LOG.error(path, e);
            throw new RuntimeException(e);
        }
    }

    public Pair<String, Stat> readData(String path) throws Exception {
        Stat stat = new Stat();
        String dataStr = new String(zkClient.getData().storingStatIn(stat).forPath(path));
        return ImmutablePair.of(dataStr, stat);
    }

    public byte[] readBinaryData(String path) throws Exception {
        Stat stat = new Stat();
        byte[] data = zkClient.getData().storingStatIn(stat).forPath(path);
        return data;
    }

    public void createPersistentPath(String path, String data) throws Exception {
        createOrUpdatePath(path, data.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT);
    }

    public void createPersistentPath(String path, byte[] data) throws Exception {
        createOrUpdatePath(path, data, CreateMode.PERSISTENT);
    }

    public void createOrUpdatePath(String path, byte[] data, CreateMode createMode) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            createPath(path, data, createMode);
        } else {
            zkClient.setData().forPath(path, data);
        }
    }

    public void createOrUpdatePath(String path, String data, CreateMode createMode) throws Exception {
        createOrUpdatePath(path, data.getBytes(StandardCharsets.UTF_8), createMode);
    }

    public void createPath(String path, String data, CreateMode createMode) throws Exception {
        createPath(path, data.getBytes(StandardCharsets.UTF_8), createMode);
    }

    public void createPath(String path, byte[] data, CreateMode createMode) throws Exception {
        zkClient.create().creatingParentsIfNeeded().withMode(createMode).
                forPath(path, data);
    }

    public void addConnectionStateListener(ConnectionStateListener listener) {
        zkClient.getConnectionStateListenable().addListener(listener);
    }

    public NodeCache watchNode(String path, NodeCacheListener nodeCacheListener) {
        NodeCache cache = new NodeCache(zkClient, path);
        cache.getListenable().addListener(nodeCacheListener);
        return cache;
    }

    @Override
    public void close() throws IOException {
        try {
            zkClient.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public String getZkUrl() {
        return zkUrl;
    }

    class SessionExpireListener implements ConnectionStateListener {
        private AbstractRegisterCallBack callBack;

        public SessionExpireListener(AbstractRegisterCallBack callBack) {
            this.callBack = callBack;
        }
        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            try {
                if (ConnectionState.RECONNECTED.equals(connectionState)) {
                    callBack.register();
                }
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

}
