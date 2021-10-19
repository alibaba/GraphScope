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

import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.ZkConfig;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
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
    private ServiceDiscovery<MaxGraphNode> serviceDiscovery;
    private List<ServiceCache<MaxGraphNode>> serviceCaches;

    private LocalNodeProvider localNodeProvider;
    private CuratorFramework curator;
    private String discoveryBasePath;
    private ExecutorService singleThreadExecutor;
    private AtomicReference<Map<RoleType, Map<Integer, MaxGraphNode>>> currentNodesRef =
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
            MaxGraphNode localNode = localNodeProvider.get();
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
            ServiceInstance<MaxGraphNode> instance =
                    ServiceInstance.<MaxGraphNode>builder()
                            .name(localNode.getRoleName())
                            .id(String.valueOf(localNode.getIdx()))
                            .payload(localNode)
                            .build();
            JsonInstanceSerializer<MaxGraphNode> serializer =
                    new JsonInstanceSerializer<>(MaxGraphNode.class);
            this.serviceDiscovery =
                    ServiceDiscoveryBuilder.builder(MaxGraphNode.class)
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
                ServiceCache<MaxGraphNode> serviceCache =
                        this.serviceDiscovery.serviceCacheBuilder().name(role.getName()).build();
                NodeChangeListener listener = new NodeChangeListener(role, serviceCache);
                serviceCache.addListener(listener, this.singleThreadExecutor);
                serviceCache.start();
                listener.cacheChanged();
                this.serviceCaches.add(serviceCache);
            }
            logger.info("ZkDiscovery started");
        } catch (Exception e) {
            throw new MaxGraphException(e);
        }
    }

    @Override
    public void stop() {
        if (this.serviceCaches != null) {
            for (ServiceCache<MaxGraphNode> serviceCache : this.serviceCaches) {
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
        private ServiceCache<MaxGraphNode> serviceCache;

        public NodeChangeListener(RoleType roleType, ServiceCache<MaxGraphNode> serviceCache) {
            this.roleType = roleType;
            this.serviceCache = serviceCache;
        }

        @Override
        public void cacheChanged() {
            logger.debug("cacheChanged. roleType [" + roleType.getName() + "]");
            synchronized (lock) {
                Map<Integer, MaxGraphNode> newRoleNodes = new HashMap<>();
                for (ServiceInstance<MaxGraphNode> instance : this.serviceCache.getInstances()) {
                    MaxGraphNode maxGraphNode = instance.getPayload();
                    newRoleNodes.put(maxGraphNode.getIdx(), maxGraphNode);
                }

                Map<RoleType, Map<Integer, MaxGraphNode>> curentNodesCopy =
                        new HashMap<>(currentNodesRef.get());
                Map<Integer, MaxGraphNode> currentRoleNodes =
                        curentNodesCopy.put(roleType, newRoleNodes);
                currentNodesRef.set(curentNodesCopy);

                if (currentRoleNodes != null && !currentRoleNodes.isEmpty()) {
                    Map<Integer, MaxGraphNode> removed = new HashMap<>();
                    currentRoleNodes.forEach(
                            (id, currentNode) -> {
                                MaxGraphNode newNode = newRoleNodes.get(id);
                                if (newNode == null || !newNode.equals(currentNode)) {
                                    removed.put(currentNode.getIdx(), currentNode);
                                }
                            });
                    notifyRemoved(roleType, removed);
                }

                if (newRoleNodes != null && !newRoleNodes.isEmpty()) {
                    Map<Integer, MaxGraphNode> added = new HashMap<>();
                    newRoleNodes.forEach(
                            (id, newNode) -> {
                                if (currentRoleNodes != null) {
                                    MaxGraphNode currentNode = currentRoleNodes.get(id);
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

    private void notifyRemoved(RoleType role, Map<Integer, MaxGraphNode> removed) {
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

    private void notifyAdded(RoleType role, Map<Integer, MaxGraphNode> added) {
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
            Map<RoleType, Map<Integer, MaxGraphNode>> currentNodes = this.currentNodesRef.get();
            for (Map.Entry<RoleType, Map<Integer, MaxGraphNode>> e : currentNodes.entrySet()) {
                RoleType role = e.getKey();
                Map<Integer, MaxGraphNode> nodes = e.getValue();
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
    public MaxGraphNode getLocalNode() {
        return this.localNodeProvider.get();
    }
}
