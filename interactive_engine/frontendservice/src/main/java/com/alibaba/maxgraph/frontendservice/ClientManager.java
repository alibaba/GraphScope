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
package com.alibaba.maxgraph.frontendservice;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.alibaba.maxgraph.common.zookeeper.ZKPaths;
import com.alibaba.maxgraph.coordinator.client.ServerDataApiClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 管理所有client,包括:
 * 1，SchemaApiClient: 处理ddl请求
 * 2，FrontendRegisterClient: frontend service向coordinator注册，并由coordinator向frontendservice
 * 通知服务ready,可以服务
 * 3，IdClient：call IdService 返回内部Id
 */
public class ClientManager {
    private static final Logger LOG = LoggerFactory.getLogger(ClientManager.class);
    private AtomicBoolean isCoordinatorRelatedClientStarted = new AtomicBoolean(false);
    private InstanceConfig instanceConfig;
//    private SchemaApiClient schemaApiClient;
    private ServerDataApiClient serverDataApiClient;
    private ConcurrentMap<Integer, Endpoint> executorEndpointMap = Maps.newConcurrentMap();
    private AtomicLong executorUpdateVersion = new AtomicLong(System.currentTimeMillis());
    private AtomicReference<List<List<Endpoint>>> endpointGroupsRef = new AtomicReference<>();


    public ClientManager(InstanceConfig instanceConfig) throws Exception {
        this.instanceConfig = instanceConfig;

        this.serverDataApiClient = new ServerDataApiClient(instanceConfig);
        Endpoint endpoint = waitAndGetCoordinatorEndpoint();
        startCoordinatorRelatedClient(endpoint);
        startCoordinatorNodeCache();
    }

    private void startCoordinatorRelatedClient(Endpoint endpoint) throws Exception {
        LOG.info("start coordinator related client of endpoint:{}", endpoint);
        this.serverDataApiClient.start();

        this.isCoordinatorRelatedClientStarted.compareAndSet(false, true);
    }

    private void startCoordinatorNodeCache() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        final NodeCache nodeCache = new NodeCache(this.serverDataApiClient.getZkNamingProxy().getZkClient(),
                ZKPaths.getCoordinatorPath(instanceConfig.getGraphName()), false);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(
                () -> {
                    if (nodeCache.getCurrentData() == null || StringUtils.isEmpty(Arrays.toString(nodeCache.getCurrentData().getData()))) {
                        LOG.info("coordinator node not exist in zk");
                    } else {
                        try {
                            Endpoint newEndpoint = JSON.fromJson(new String(nodeCache.getCurrentData().getData()), Endpoint.class);
                            LOG.info("current coordinator endpoint:{}", newEndpoint);
                            startCoordinatorRelatedClient(newEndpoint);
                        } catch (Exception e) {
                            LOG.warn("{}", e);
                        }

                    }
                },
                pool
        );
    }

    public boolean isCoordinatorRelatedClientNotReady() {
        return !isCoordinatorRelatedClientStarted.get();
    }

    public ServerDataApiClient getServerDataApiClient() {
        return serverDataApiClient;
    }

    public void updateExecutorMap(int workerId, Endpoint endpoint) {
        Endpoint existEndpoint = executorEndpointMap.get(workerId);
        if (!endpoint.equals(existEndpoint)) {
            executorEndpointMap.put(workerId, endpoint);
            executorUpdateVersion.set(System.currentTimeMillis());
        }
    }

    public List<Endpoint> getExecutorList() {
        return Lists.newArrayList(executorEndpointMap.values());
    }

    public Endpoint getServiceExecutor() {
        return checkNotNull(executorEndpointMap.get(1), "Get service executor fail from executor map=>" + executorEndpointMap);
    }

    public int getExecutorCount() {
        return executorEndpointMap.size();
    }

    public Endpoint getExecutor(int workerId) {
        return checkNotNull(executorEndpointMap.get(workerId), "Get executor " +  workerId + " fail from executor map=>" + executorEndpointMap);
    }

    public long getExecutorUpdateVersion() {
        return this.executorUpdateVersion.get();
    }

    public void updateGroupExecutors() {
        int replicaCount = this.instanceConfig.getReplicaCount();
        List<List<Endpoint>> endpointGroups = new ArrayList<>(replicaCount);
        int executorCnt = this.instanceConfig.getResourceExecutorCount() / replicaCount;
        for (int r = 0; r < replicaCount; r++) {
            List<Endpoint> endpoints = new ArrayList<>();
            for (int i = 0; i < executorCnt; i++) {
                endpoints.add(executorEndpointMap.get(r * executorCnt + i + 1));
            }
            endpointGroups.add(endpoints);
        }
        this.endpointGroupsRef.set(endpointGroups);
    }


    public List<List<Endpoint>> getEndpointGroups() {
        return this.endpointGroupsRef.get();
    }

    private Endpoint waitAndGetCoordinatorEndpoint() {
        Endpoint endpoint;
        while ((endpoint = this.serverDataApiClient.getZkNamingProxy().getCoordinatorEndpoint()) == null) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOG.error("thread sleep fail", e);
            }
        }
        return endpoint;
    }
}
