package com.alibaba.maxgraph.v2.store.executor;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.StoreConfig;
import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.discovery.ZkDiscovery;
import com.alibaba.maxgraph.v2.common.util.ThreadFactoryUtils;
import com.alibaba.maxgraph.v2.store.GraphPartition;
import com.alibaba.maxgraph.v2.store.StoreService;
import com.alibaba.maxgraph.v2.store.executor.jna.ExecutorLibrary;
import com.alibaba.maxgraph.v2.store.jna.JnaGraphStore;
import com.sun.jna.Pointer;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorService implements Cloneable {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorService.class);

    private Configs configs;
    private CuratorFramework curator;
    private StoreService storeService;
    private ExecutorManager executorManager;
    private Executor executor = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("executor-service-pool", logger));

    public ExecutorService(Configs configs, StoreService storeService, CuratorFramework curator,
                           NodeDiscovery discovery) {
        this.configs = configs;
        this.curator = curator;
        this.storeService = storeService;
        ExecutorDiscoveryManager discoveryManager;
        if (CommonConfig.DISCOVERY_MODE.get(configs).equalsIgnoreCase("file")) {
            LocalNodeProvider provider = new LocalNodeProvider(configs);
            discoveryManager = new ExecutorDiscoveryManager(provider, discovery, provider, discovery, provider,
                    discovery, provider, discovery);
        } else {
            LocalNodeProvider engineServerProvider = new LocalNodeProvider(RoleType.EXECUTOR_ENGINE, configs);
            NodeDiscovery engineServerDiscovery = new ZkDiscovery(configs, engineServerProvider, this.curator);
            LocalNodeProvider storeQueryProvider = new LocalNodeProvider(RoleType.EXECUTOR_GRAPH, configs);
            NodeDiscovery storeQueryDiscovery = new ZkDiscovery(configs, storeQueryProvider, this.curator);
            LocalNodeProvider queryExecuteProvider = new LocalNodeProvider(RoleType.EXECUTOR_QUERY, configs);
            NodeDiscovery queryExecuteDiscovery = new ZkDiscovery(configs, queryExecuteProvider, this.curator);
            LocalNodeProvider queryManageProvider = new LocalNodeProvider(RoleType.EXECUTOR_MANAGE, configs);
            NodeDiscovery queryManageDiscovery = new ZkDiscovery(configs, queryManageProvider, this.curator);
            discoveryManager = new ExecutorDiscoveryManager(engineServerProvider,
                    engineServerDiscovery,
                    storeQueryProvider,
                    storeQueryDiscovery,
                    queryExecuteProvider,
                    queryExecuteDiscovery,
                    queryManageProvider,
                    queryManageDiscovery);
        }
        this.executorManager = new ExecutorManager(configs, discoveryManager);
    }

    public void start() {
        logger.info("Start to launch executor service");
        int nodeCount = CommonConfig.STORE_NODE_COUNT.get(this.configs);
        int workerPerProcess = ExecutorConfig.EXECUTOR_WORKER_PER_PROCESS.get(this.configs);

        Configs executorConfig = Configs.newBuilder()
                .put("node.idx", String.valueOf(CommonConfig.NODE_IDX.get(configs)))
                .put("graph.name", CommonConfig.GRAPH_NAME.get(configs))
                .put("partition.count", String.valueOf(CommonConfig.PARTITION_COUNT.get(configs)))
                .put("graph.port", String.valueOf(StoreConfig.EXECUTOR_GRAPH_PORT.get(configs)))
                .put("query.port", String.valueOf(StoreConfig.EXECUTOR_QUERY_PORT.get(configs)))
                .put("engine.port", String.valueOf(StoreConfig.EXECUTOR_ENGINE_PORT.get(configs)))
                .build();
        byte[] configBytes = executorConfig.toProto().toByteArray();

        logger.info("Start to open executor server");
        Pointer pointer = ExecutorLibrary.INSTANCE.openExecutorServer(configBytes, configBytes.length);
        // Add graph store with partition id to executor
        Map<Integer, GraphPartition> idToPartition = this.storeService.getIdToPartition();
        logger.info("Get partition count " + idToPartition.size() + " for current store node");
        for (Map.Entry<Integer, GraphPartition> partition : idToPartition.entrySet()) {
            int partitionId = partition.getKey();
            GraphPartition graphStore = partition.getValue();
            if (graphStore instanceof JnaGraphStore) {
                JnaGraphStore jnaGraphStore = (JnaGraphStore) graphStore;
                ExecutorLibrary.INSTANCE.addGraphPartition(pointer, partitionId, jnaGraphStore.getPointer());
                logger.info("Add partition " + partitionId);
            }
        }
        // Assign partition id to each worker id
        MetaService metaService = this.storeService.getMetaService();
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
        this.executorManager.getDiscoveryManager().getEngineServerDiscovery().addListener(new EngineServerListener(this.executorManager));

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

    public void close() {
        executor.execute(() -> {
            this.executorManager.stopEngineServer();
            this.executorManager.stopRpcServer();
        });
    }
}
