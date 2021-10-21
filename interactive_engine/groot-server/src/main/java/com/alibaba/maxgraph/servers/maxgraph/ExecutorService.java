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
package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.alibaba.graphscope.groot.discovery.*;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.maxgraph.servers.AbstractService;
import com.alibaba.maxgraph.servers.jna.ExecutorLibrary;
import com.alibaba.graphscope.groot.store.jna.JnaGraphStore;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorService implements AbstractService {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorService.class);

    private Configs configs;
    private StoreService storeService;
    private ExecutorManager executorManager;
    private MetaService metaService;
    private Executor executor =
            new ThreadPoolExecutor(
                    1,
                    1,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                            "executor-service-pool", logger));

    public ExecutorService(
            Configs configs,
            StoreService storeService,
            DiscoveryFactory discoveryFactory,
            MetaService metaService) {
        this.configs = configs;
        this.storeService = storeService;
        this.metaService = metaService;

        LocalNodeProvider engineServerProvider =
                new LocalNodeProvider(RoleType.EXECUTOR_ENGINE, configs);
        LocalNodeProvider storeQueryProvider =
                new LocalNodeProvider(RoleType.EXECUTOR_GRAPH, configs);
        LocalNodeProvider queryExecuteProvider =
                new LocalNodeProvider(RoleType.EXECUTOR_QUERY, configs);
        LocalNodeProvider queryManageProvider =
                new LocalNodeProvider(RoleType.EXECUTOR_MANAGE, configs);

        ExecutorDiscoveryManager discoveryManager =
                new ExecutorDiscoveryManager(
                        engineServerProvider,
                        discoveryFactory.makeDiscovery(engineServerProvider),
                        storeQueryProvider,
                        discoveryFactory.makeDiscovery(storeQueryProvider),
                        queryExecuteProvider,
                        discoveryFactory.makeDiscovery(queryExecuteProvider),
                        queryManageProvider,
                        discoveryFactory.makeDiscovery(queryManageProvider));
        this.executorManager = new ExecutorManager(configs, discoveryManager);
    }

    public void start() {
        logger.info("Start to launch executor service");
        int nodeCount = CommonConfig.STORE_NODE_COUNT.get(this.configs);
        int workerPerProcess = ExecutorConfig.EXECUTOR_WORKER_PER_PROCESS.get(this.configs);

        Configs executorConfig =
                Configs.newBuilder()
                        .put("node.idx", String.valueOf(CommonConfig.NODE_IDX.get(configs)))
                        .put("graph.name", CommonConfig.GRAPH_NAME.get(configs))
                        .put(
                                "partition.count",
                                String.valueOf(CommonConfig.PARTITION_COUNT.get(configs)))
                        .put(
                                "graph.port",
                                String.valueOf(StoreConfig.EXECUTOR_GRAPH_PORT.get(configs)))
                        .put(
                                "query.port",
                                String.valueOf(StoreConfig.EXECUTOR_QUERY_PORT.get(configs)))
                        .put(
                                "engine.port",
                                String.valueOf(StoreConfig.EXECUTOR_ENGINE_PORT.get(configs)))
                        .put("worker.per.process", String.valueOf(workerPerProcess))
                        .put("worker.num", String.valueOf(nodeCount))
                        .build();
        byte[] configBytes = executorConfig.toProto().toByteArray();

        logger.info("Start to open executor server");
        Pointer pointer =
                ExecutorLibrary.INSTANCE.openExecutorServer(configBytes, configBytes.length);
        // Add graph store with partition id to executor
        Map<Integer, GraphPartition> idToPartition = this.storeService.getIdToPartition();
        logger.info("Get partition count " + idToPartition.size() + " for current store node");
        for (Map.Entry<Integer, GraphPartition> partition : idToPartition.entrySet()) {
            int partitionId = partition.getKey();
            GraphPartition graphStore = partition.getValue();
            if (graphStore instanceof JnaGraphStore) {
                JnaGraphStore jnaGraphStore = (JnaGraphStore) graphStore;
                ExecutorLibrary.INSTANCE.addGraphPartition(
                        pointer, partitionId, jnaGraphStore.getPointer());
                logger.info("Add partition " + partitionId);
            }
        }
        // Assign partition id to each worker id
        for (int i = 0; i < nodeCount; i++) {
            List<Integer> partitionIdList = metaService.getPartitionsByStoreId(i);
            logger.info("Get partition id list " + partitionIdList + " for store index " + i);
            partitionIdList.sort(Integer::compareTo);
            for (int k = 0; k < partitionIdList.size(); k++) {
                int partitionId = partitionIdList.get(k);
                int innnerIndex = k % workerPerProcess;
                int workerId = (i * workerPerProcess) + innnerIndex;
                logger.info("Add partition->worker mapping " + partitionId + "->" + workerId);
                ExecutorLibrary.INSTANCE.addPartitionWorkerMapping(pointer, partitionId, workerId);
            }
        }

        this.executorManager.initialExecutor(pointer, nodeCount);
        this.executorManager.startEngineServer();
        this.executorManager.startRpcServer();
        this.executorManager
                .getDiscoveryManager()
                .getEngineServerDiscovery()
                .addListener(new EngineServerListener(this.executorManager));

        while (!this.executorManager.checkEngineServersConnect()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("Wait all the engine server to build connection with each other");
        }
        logger.info("All engine server build connection with each success");
    }

    @Override
    public void stop() {
        executor.execute(
                () -> {
                    this.executorManager.stopEngineServer();
                    this.executorManager.stopRpcServer();
                });
    }
}
