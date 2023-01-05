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

package com.alibaba.graphscope.groot.servers.ir;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.compiler.api.schema.SchemaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.FrontendConfig;
import com.alibaba.graphscope.groot.common.config.GremlinConfig;
import com.alibaba.graphscope.groot.discovery.DiscoveryFactory;
import com.alibaba.graphscope.groot.frontend.SnapshotUpdateCommitter;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.servers.AbstractService;
import com.alibaba.graphscope.groot.servers.ComputeServiceProducer;
import com.alibaba.graphscope.groot.store.StoreService;

import java.util.HashMap;
import java.util.Map;

public class IrServiceProducer implements ComputeServiceProducer {
    private Configs configs;

    public IrServiceProducer(Configs configs) {
        this.configs = configs;
    }

    @Override
    public AbstractService makeGraphService(
            SchemaFetcher schemaFetcher, ChannelManager channelManager) {
        int executorCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        RpcChannelFetcher channelFetcher =
                new RpcChannelManagerFetcher(channelManager, executorCount, RoleType.GAIA_RPC);
        com.alibaba.graphscope.common.config.Configs irConfigs = getConfigs();
        IrMetaFetcher irMetaFetcher = new GrootMetaFetcher(schemaFetcher);

        SnapshotUpdateCommitter updateCommitter = new SnapshotUpdateCommitter(channelManager);

        int frontendId = CommonConfig.NODE_IDX.get(configs);
        FrontendQueryManager queryManager =
                new FrontendQueryManager(irMetaFetcher, frontendId, updateCommitter);

        return new AbstractService() {
            private IrGremlinServer irGremlinServer =
                    new IrGremlinServer(GremlinConfig.GREMLIN_PORT.get(configs));

            @Override
            public void start() {
                try {
                    irGremlinServer.start(
                            irConfigs,
                            irMetaFetcher,
                            channelFetcher,
                            queryManager,
                            TestGraphFactory.GROOT);

                    queryManager.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void stop() {
                try {
                    irGremlinServer.close();

                    queryManager.stop();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public AbstractService makeExecutorService(
            StoreService storeService, MetaService metaService, DiscoveryFactory discoveryFactory) {
        ExecutorEngine executorEngine = new GaiaEngine(configs, discoveryFactory);
        return new GaiaService(configs, executorEngine, storeService, metaService);
    }

    private com.alibaba.graphscope.common.config.Configs getConfigs() {
        Map<String, String> configMap = new HashMap<>();

        int executorCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        configMap.put(PegasusConfig.PEGASUS_SERVER_NUM.getKey(), String.valueOf(executorCount));

        addToConfigMapIfExist(PegasusConfig.PEGASUS_WORKER_NUM.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_TIMEOUT.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_BATCH_SIZE.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_MEMORY_LIMIT.getKey(), configMap);
        // add authentication
        addToConfigMapIfExist(FrontendConfig.AUTH_USERNAME.getKey(), configMap);
        addToConfigMapIfExist(FrontendConfig.AUTH_PASSWORD.getKey(), configMap);

        return new com.alibaba.graphscope.common.config.Configs(configMap);
    }

    private void addToConfigMapIfExist(String key, Map<String, String> configMap) {
        String value = configs.get(key);
        if (value != null) {
            configMap.put(key, value);
        }
    }
}
