package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.compiler.schema.JsonFileSchemaFetcher;
import com.alibaba.maxgraph.frontendservice.RemoteGraph;
import com.alibaba.maxgraph.frontendservice.server.ExecutorAddressFetcher;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class Frontend extends com.alibaba.maxgraph.frontendservice.Frontend {
    private static final Logger logger = LoggerFactory.getLogger(Frontend.class);
    private IrGremlinServer gremlinServer;

    public Frontend(InstanceConfig instanceConfig) throws Exception {
        super(instanceConfig);
    }

    @Override
    protected void initAndStartGremlinServer() throws Exception {
        SchemaFetcher schemaFetcher;
        String vineyardSchemaPath = this.instanceConfig.getVineyardSchemaPath();

        logger.info("Read schema from vineyard schema file " + vineyardSchemaPath);
        schemaFetcher = new JsonFileSchemaFetcher(vineyardSchemaPath);

        this.remoteGraph = new RemoteGraph(this, schemaFetcher);
        this.remoteGraph.refresh();

        this.graph = new TinkerMaxGraph(instanceConfig, remoteGraph, new DefaultGraphDfs());

        // add ir compiler
        Configs configs = getConfigs(this.instanceConfig);
        IrMetaFetcher irMetaFetcher = getStoreConfigs(this.instanceConfig);

        RpcAddressFetcher addressFetcher = new ExecutorAddressFetcher(this.clientManager);
        RpcChannelFetcher channelFetcher = new RpcAddressChannelFetcher(addressFetcher);

        this.gremlinServer = new IrGremlinServer(this.instanceConfig.getGremlinServerPort());
        this.gremlinServer.start(
                configs,
                irMetaFetcher,
                channelFetcher,
                new IrMetaQueryCallback(irMetaFetcher),
                TestGraphFactory.VINEYARD);

        this.gremlinServerPort = gremlinServer.getGremlinServerPort();
    }

    @Override
    public void start() throws Exception {
        queryManager.start();
        startRpcService();
        startHBThread();
        this.gremlinExecutor = gremlinServer.getGremlinExecutor();
    }

    private Configs getConfigs(InstanceConfig instanceConfig) {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(
                PegasusConfig.PEGASUS_WORKER_NUM.getKey(),
                String.valueOf(instanceConfig.getPegasusWorkerNum()));
        configMap.put(
                PegasusConfig.PEGASUS_TIMEOUT.getKey(),
                String.valueOf(instanceConfig.getPegasusTimeoutMS()));
        configMap.put(
                PegasusConfig.PEGASUS_BATCH_SIZE.getKey(),
                String.valueOf(instanceConfig.getPegasusBatchSize()));
        configMap.put(
                PegasusConfig.PEGASUS_OUTPUT_CAPACITY.getKey(),
                String.valueOf(instanceConfig.getPegasusOutputCapacity()));
        configMap.put(
                PegasusConfig.PEGASUS_MEMORY_LIMIT.getKey(),
                String.valueOf(instanceConfig.getPegasusMemoryLimit()));
        configMap.put(
                PegasusConfig.PEGASUS_SERVER_NUM.getKey(),
                String.valueOf(instanceConfig.getResourceExecutorCount()));
        return new Configs(configMap);
    }

    private IrMetaFetcher getStoreConfigs(InstanceConfig instanceConfig) {
        return new VineyardMetaFetcher(instanceConfig);
    }

    public static void main(String[] args) {
        try {
            logger.info("start to run FrontendServiceMain.");
            Frontend frontend = new Frontend(CommonUtil.getInstanceConfig(args, 101));
            frontend.start();
            CountDownLatch shutdown = new CountDownLatch(1);
            shutdown.await();
        } catch (Throwable t) {
            logger.error("Error in worker main:", t);
            System.exit(1);
        }

        System.exit(0);
    }
}
