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
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 2017/3/29.
 *
 * @author beimian.mcq
 */
public class ZkClient implements CuratorFramework {
    private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);
    private final CuratorFramework delegate;
    private final String zkUrl;
    private final AtomicInteger REFERENCE_COUNT = new AtomicInteger(0);
    private ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(500, 3);
    public ZkClient(InstanceConfig instanceConfig) {
        this(instanceConfig.getZkConnect(), instanceConfig.getZkConnectionTimeoutMs(), instanceConfig
                        .getZkSessionTimeoutMs(), instanceConfig.getZkAuthEnable(), instanceConfig.getZkAuthUser(),
                instanceConfig.getZkAuthPassword());
    }

    public ZkClient(String zkUrl, int connectionTimeout, int sessionTimeout, boolean zkAuthEnable, String user,
                    String password) {
        this.zkUrl = zkUrl;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(zkUrl)
                .retryPolicy(this.exponentialBackoffRetry)
                .connectionTimeoutMs(connectionTimeout)
                .sessionTimeoutMs(sessionTimeout);
        if (zkAuthEnable) {
            String zkAuthString = user + ":" + password;
            builder.authorization("digest", zkAuthString.getBytes())
                    .aclProvider(new ACLProvider() {
                        @Override
                        public List<ACL> getDefaultAcl() {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }

                        @Override
                        public List<ACL> getAclForPath(String path) {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    });
        }

        this.delegate = builder.build();
    }


    public void increaseRef() {
        REFERENCE_COUNT.getAndIncrement();
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT.decrementAndGet() == 0) {
            LOG.info("Close zk connect to {}", zkUrl);
            delegate.close();
        }
    }

    public boolean isClosed() {
        return this.getState() == CuratorFrameworkState.STOPPED;
    }

    public boolean forceClose() {
        try {
            REFERENCE_COUNT.set(0);
            delegate.close();
            LOG.info("Force closed zk connect {}", zkUrl);
            return true;
        } catch (Exception e) {
            LOG.error("Close zk connect error ", e);
            return false;
        }
    }

    @Override
    public CuratorFrameworkState getState() {
        return delegate.getState();
    }

    @Override
    public boolean isStarted() {
        return delegate.isStarted();
    }

    @Override
    public CreateBuilder create() {
        return delegate.create();
    }

    @Override
    public DeleteBuilder delete() {
        return delegate.delete();
    }

    @Override
    public ExistsBuilder checkExists() {
        return delegate.checkExists();
    }

    @Override
    public GetDataBuilder getData() {
        return delegate.getData();
    }

    @Override
    public SetDataBuilder setData() {
        return delegate.setData();
    }

    @Override
    public GetChildrenBuilder getChildren() {
        return delegate.getChildren();
    }

    @Override
    public GetACLBuilder getACL() {
        return delegate.getACL();
    }

    @Override
    public SetACLBuilder setACL() {
        return delegate.setACL();
    }

    @Override
    public CuratorTransaction inTransaction() {
        return delegate.inTransaction();
    }

    @Override
    public void sync(String path, Object backgroundContextObject) {
        delegate.sync();
    }

    @Override
    public void createContainers(String path) throws Exception {
        delegate.createContainers(path);
    }

    @Override
    public SyncBuilder sync() {
        return delegate.sync();
    }

    @Override
    public Listenable<ConnectionStateListener> getConnectionStateListenable() {
        return delegate.getConnectionStateListenable();
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable() {
        return delegate.getCuratorListenable();
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() {
        return delegate.getUnhandledErrorListenable();
    }

    @Override
    public CuratorFramework nonNamespaceView() {
        return delegate.nonNamespaceView();
    }

    @Override
    public CuratorFramework usingNamespace(String newNamespace) {
        return delegate.usingNamespace(newNamespace);
    }

    @Override
    public String getNamespace() {
        return delegate.getNamespace();
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient() {
        return delegate.getZookeeperClient();
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path) {
        return delegate.newNamespaceAwareEnsurePath(path);
    }

    @Override
    public void clearWatcherReferences(Watcher watcher) {
        delegate.clearWatcherReferences(watcher);
    }

    @Override
    public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException {
        if (isClosed()) {
            throw new RuntimeException("Client has been closed");
        }
        LOG.info("Wait zookeeper server connected ...");
        return delegate.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void blockUntilConnected() throws InterruptedException {
        boolean isConnected = false;
        while (!isConnected) {
            isConnected = blockUntilConnected(60, TimeUnit.SECONDS);
        }
    }
}
