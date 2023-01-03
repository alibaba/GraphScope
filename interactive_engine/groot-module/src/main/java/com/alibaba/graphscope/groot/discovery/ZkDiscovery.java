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
package com.alibaba.graphscope.groot.discovery;

import com.alibaba.graphscope.common.RoleType;
import com.alibaba.graphscope.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.compiler.api.exception.GrootException;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.ZkConfig;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ZkDiscovery implements NodeDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ZkDiscovery.class);
    public static final String ROOT_NODE = "discovery";

    private List<Listener> listeners = new ArrayList<>();
    private ServiceDiscovery<GrootNode> serviceDiscovery;
    private List<ServiceCache<GrootNode>> serviceCaches;

    private LocalNodeProvider localNodeProvider;
    private CuratorFramework curator;
    private String discoveryBasePath;
    private ExecutorService singleThreadExecutor;
    private AtomicReference<Map<RoleType, Map<Integer, GrootNode>>> currentNodesRef =
            new AtomicReference<>(new HashMap<>());

    private Object lock = new Object();

    public ZkDiscovery(
            Configs configs, LocalNodeProvider localNodeProvider, CuratorFramework curator) {
        this(ZkConfig.ZK_BASE_PATH.get(configs), localNodeProvider, curator);
    }

    public ZkDiscovery(
            String basePath, LocalNodeProvider localNodeProvider, CuratorFramework curator) {
        this.localNodeProvider = localNodeProvider;
        this.curator = curator;
        this.discoveryBasePath = ZKPaths.makePath(basePath, ROOT_NODE);
    }

    @Override
    public void start() {
        try {
            GrootNode localNode = localNodeProvider.get();
            this.currentNodesRef = new AtomicReference<>(new HashMap<>());
            this.singleThreadExecutor =
                    new ThreadPoolExecutor(
                            1,
                            1,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(),
                            ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                    "zk-discovery", logger));
            ServiceInstance<GrootNode> instance =
                    ServiceInstance.<GrootNode>builder()
                            .name(localNode.getRoleName())
                            .id(String.valueOf(localNode.getIdx()))
                            .payload(localNode)
                            .build();
            JsonInstanceSerializer<GrootNode> serializer =
                    new JsonInstanceSerializer<>(GrootNode.class);

            this.serviceDiscovery =
                    ServiceDiscoveryBuilder.builder(GrootNode.class)
                            .client(curator)
                            .basePath(discoveryBasePath)
                            .serializer(serializer)
                            .thisInstance(instance)
                            .build();
            logger.debug(
                    "Start to add node "
                            + localNode
                            + " to discovery with base path "
                            + discoveryBasePath);
            this.serviceDiscovery.start();

            RoleType[] roleTypes = RoleType.values();
            this.serviceCaches = new ArrayList<>(roleTypes.length);
            for (RoleType role : roleTypes) {
                ServiceCache<GrootNode> serviceCache =
                        this.serviceDiscovery.serviceCacheBuilder().name(role.getName()).build();
                NodeChangeListener listener = new NodeChangeListener(role, serviceCache);
                serviceCache.addListener(listener, this.singleThreadExecutor);
                serviceCache.start();
                listener.cacheChanged();
                this.serviceCaches.add(serviceCache);
            }
            logger.info("ZkDiscovery started");
        } catch (Exception e) {
            throw new GrootException(e);
        }
    }

    @Override
    public void stop() {
        if (this.serviceCaches != null) {
            for (ServiceCache<GrootNode> serviceCache : this.serviceCaches) {
                try {
                    serviceCache.close();
                } catch (Exception e) {
                    logger.warn("close serviceCache failed", e);
                }
            }
            this.serviceCaches = null;
        }
        if (this.serviceDiscovery != null) {
            try {
                this.serviceDiscovery.close();
            } catch (Exception e) {
                logger.warn("close serviceDiscovery failed", e);
            }
        }
        logger.info("ZkDiscovery stopped");
    }

    private class NodeChangeListener implements ServiceCacheListener {

        private RoleType roleType;
        private ServiceCache<GrootNode> serviceCache;

        public NodeChangeListener(RoleType roleType, ServiceCache<GrootNode> serviceCache) {
            this.roleType = roleType;
            this.serviceCache = serviceCache;
        }

        @Override
        public void cacheChanged() {
            logger.debug("cacheChanged. roleType [" + roleType.getName() + "]");
            synchronized (lock) {
                Map<Integer, GrootNode> newRoleNodes = new HashMap<>();
                for (ServiceInstance<GrootNode> instance : this.serviceCache.getInstances()) {
                    GrootNode grootNode = instance.getPayload();
                    newRoleNodes.put(grootNode.getIdx(), grootNode);
                }

                Map<RoleType, Map<Integer, GrootNode>> currentNodesCopy =
                        new HashMap<>(currentNodesRef.get());
                Map<Integer, GrootNode> currentRoleNodes =
                        currentNodesCopy.put(roleType, newRoleNodes);
                currentNodesRef.set(currentNodesCopy);

                if (currentRoleNodes != null && !currentRoleNodes.isEmpty()) {
                    Map<Integer, GrootNode> removed = new HashMap<>();
                    currentRoleNodes.forEach(
                            (id, currentNode) -> {
                                GrootNode newNode = newRoleNodes.get(id);
                                if (newNode == null || !newNode.equals(currentNode)) {
                                    removed.put(currentNode.getIdx(), currentNode);
                                }
                            });
                    notifyRemoved(roleType, removed);
                }

                if (newRoleNodes != null && !newRoleNodes.isEmpty()) {
                    Map<Integer, GrootNode> added = new HashMap<>();
                    newRoleNodes.forEach(
                            (id, newNode) -> {
                                if (currentRoleNodes != null) {
                                    GrootNode currentNode = currentRoleNodes.get(id);
                                    if (currentNode != null && currentNode.equals(newNode)) {
                                        return;
                                    }
                                }
                                added.put(newNode.getIdx(), newNode);
                            });
                    notifyAdded(roleType, added);
                }
            }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            logger.info("stateChanged to [" + newState + "]");
        }
    }

    private void notifyRemoved(RoleType role, Map<Integer, GrootNode> removed) {
        if (removed.isEmpty()) {
            return;
        }
        logger.debug("role [" + role.getName() + "] remove nodes [" + removed.values() + "]");
        for (Listener listener : this.listeners) {
            this.singleThreadExecutor.execute(
                    () -> {
                        try {
                            listener.nodesLeft(role, removed);
                        } catch (Exception e) {
                            logger.error(
                                    "listener ["
                                            + listener
                                            + "] failed on nodesLeft ["
                                            + removed
                                            + "]",
                                    e);
                        }
                    });
        }
    }

    private void notifyAdded(RoleType role, Map<Integer, GrootNode> added) {
        if (added.isEmpty()) {
            return;
        }
        logger.debug("role [" + role.getName() + "] add nodes [" + added.values() + "]");
        for (Listener listener : this.listeners) {
            this.singleThreadExecutor.execute(
                    () -> {
                        try {
                            listener.nodesJoin(role, added);
                        } catch (Exception e) {
                            logger.error(
                                    "listener ["
                                            + listener
                                            + "] failed on nodesJoin ["
                                            + added
                                            + "]",
                                    e);
                        }
                    });
        }
    }

    @Override
    public void addListener(Listener listener) {
        synchronized (lock) {
            this.listeners.add(listener);
            Map<RoleType, Map<Integer, GrootNode>> currentNodes = this.currentNodesRef.get();
            for (Map.Entry<RoleType, Map<Integer, GrootNode>> e : currentNodes.entrySet()) {
                RoleType role = e.getKey();
                Map<Integer, GrootNode> nodes = e.getValue();
                if (!nodes.isEmpty()) {
                    this.singleThreadExecutor.execute(
                            () -> {
                                try {
                                    listener.nodesJoin(role, nodes);
                                } catch (Exception ex) {
                                    logger.error(
                                            "listener ["
                                                    + listener
                                                    + "] failed on nodesJoin ["
                                                    + nodes
                                                    + "]",
                                            ex);
                                }
                            });
                }
            }
        }
    }

    @Override
    public void removeListener(Listener listener) {
        synchronized (lock) {
            this.listeners.remove(listener);
        }
    }

    @Override
    public GrootNode getLocalNode() {
        return this.localNodeProvider.get();
    }
}
