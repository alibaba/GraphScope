package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.store.StoreConfigs;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.frontendservice.ClientManager;
import com.alibaba.maxgraph.frontendservice.server.ExecutorAddressFetcher;

import java.util.HashMap;
import java.util.Map;

// for vineyard service
public class IrGremlinServiceMain {
    public IrGremlinServiceMain(InstanceConfig instanceConfig) throws Exception {
        Configs configs = getConfigs(instanceConfig);

        StoreConfigs storeConfigs = getStoreConfigs(instanceConfig);

        ClientManager clientManager = new ClientManager(instanceConfig);
        RpcAddressFetcher addressFetcher = new ExecutorAddressFetcher(clientManager);
        RpcChannelFetcher channelFetcher = new RpcAddressChannelFetcher(addressFetcher);

        IrGremlinServer gremlinServer = new IrGremlinServer();
        gremlinServer.start(configs, storeConfigs, channelFetcher);
    }

    private Configs getConfigs(InstanceConfig instanceConfig) {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(PegasusConfig.PEGASUS_WORKER_NUM.getKey(), String.valueOf(instanceConfig.getPegasusWorkerNum()));
        configMap.put(PegasusConfig.PEGASUS_TIMEOUT.getKey(), String.valueOf(instanceConfig.getPegasusTimeoutMS()));
        configMap.put(PegasusConfig.PEGASUS_BATCH_SIZE.getKey(), String.valueOf(instanceConfig.getPegasusBatchSize()));
        configMap.put(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.getKey(), String.valueOf(instanceConfig.getPegasusOutputCapacity()));
        configMap.put(PegasusConfig.PEGASUS_MEMORY_LIMIT.getKey(), String.valueOf(instanceConfig.getPegasusMemoryLimit()));
        return new Configs(configMap);
    }

    private StoreConfigs getStoreConfigs(InstanceConfig instanceConfig) {
        return new VineyardStoreConfigs(instanceConfig);
    }
}
