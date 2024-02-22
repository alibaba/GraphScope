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

import com.alibaba.graphscope.GraphServer;
import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.config.AuthConfig;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.schema.api.SchemaFetcher;
import com.alibaba.graphscope.groot.discovery.DiscoveryFactory;
import com.alibaba.graphscope.groot.frontend.SnapshotUpdateCommitter;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.servers.AbstractService;
import com.alibaba.graphscope.groot.servers.ComputeServiceProducer;
import com.alibaba.graphscope.groot.store.StoreService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IrServiceProducer implements ComputeServiceProducer {
    private static final Logger logger = LoggerFactory.getLogger(IrServiceProducer.class);
    private final Configs configs;

    public IrServiceProducer(Configs configs) {
        this.configs = configs;
    }

    @Override
    public AbstractService makeGraphService(
            SchemaFetcher schemaFetcher, ChannelManager channelManager) {
        int executorCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        ChannelFetcher channelFetcher =
                new RpcChannelManagerFetcher(channelManager, executorCount, RoleType.GAIA_RPC);
        com.alibaba.graphscope.common.config.Configs irConfigs = getConfigs();
        logger.info("IR configs: {}", irConfigs);
        IrMetaFetcher irMetaFetcher = new GrootMetaFetcher(schemaFetcher);
        SnapshotUpdateCommitter updateCommitter = new SnapshotUpdateCommitter(channelManager);
        int frontendId = CommonConfig.NODE_IDX.get(configs);
        FrontendQueryManager queryManager =
                new FrontendQueryManager(irMetaFetcher, frontendId, updateCommitter);

        return new AbstractService() {
            private GraphServer graphServer =
                    new GraphServer(
                            irConfigs, channelFetcher, queryManager, TestGraphFactory.GROOT);

            @Override
            public void start() {
                try {
                    this.graphServer.start();
                    queryManager.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void stop() {
                try {
                    if (this.graphServer != null) {
                        this.graphServer.close();
                    }
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
        // add pegasus config
        addToConfigMapIfExist(PegasusConfig.PEGASUS_HOSTS.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_WORKER_NUM.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_BATCH_SIZE.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.getKey(), configMap);
        addToConfigMapIfExist(PegasusConfig.PEGASUS_MEMORY_LIMIT.getKey(), configMap);
        // add authentication
        addToConfigMapIfExist(AuthConfig.AUTH_USERNAME.getKey(), configMap);
        addToConfigMapIfExist(AuthConfig.AUTH_PASSWORD.getKey(), configMap);
        // add gremlin config
        addToConfigMapIfExist(FrontendConfig.GREMLIN_SERVER_DISABLED.getKey(), configMap);
        addToConfigMapIfExist(FrontendConfig.GREMLIN_SERVER_PORT.getKey(), configMap);
        // add neo4j config
        addToConfigMapIfExist(FrontendConfig.NEO4J_BOLT_SERVER_DISABLED.getKey(), configMap);
        addToConfigMapIfExist(FrontendConfig.NEO4J_BOLT_SERVER_PORT.getKey(), configMap);
        // add timeout config
        addToConfigMapIfExist(FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.getKey(), configMap);
        // add frontend server id
        addToConfigMapIfExist(FrontendConfig.FRONTEND_SERVER_ID.getKey(), configMap);
        // add frontend server num
        addToConfigMapIfExist(FrontendConfig.FRONTEND_SERVER_NUM.getKey(), configMap);
        // add frontend qps limit
        addToConfigMapIfExist(FrontendConfig.QUERY_PER_SECOND_LIMIT.getKey(), configMap);
        return new com.alibaba.graphscope.common.config.Configs(configMap);
    }

    private void addToConfigMapIfExist(String key, Map<String, String> configMap) {
        String value = configs.get(key);
        if (value != null) {
            configMap.put(key, value);
        }
    }
}
