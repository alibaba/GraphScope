/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
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

        IrMetaFetcher irMetaFetcher = getStoreConfigs(instanceConfig);

        ClientManager clientManager = new ClientManager(instanceConfig);
        RpcAddressFetcher addressFetcher = new ExecutorAddressFetcher(clientManager);
        RpcChannelFetcher channelFetcher = new RpcAddressChannelFetcher(addressFetcher);

        IrGremlinServer gremlinServer = new IrGremlinServer(instanceConfig.getGremlinServerPort());
        gremlinServer.start(configs, irMetaFetcher, channelFetcher);
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
}
