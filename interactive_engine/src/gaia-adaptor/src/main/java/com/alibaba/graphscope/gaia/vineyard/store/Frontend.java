/*
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
package com.alibaba.graphscope.gaia.vineyard.store;

import com.alibaba.graphscope.gaia.AsyncRpcBroadcastProcessor;
import com.alibaba.graphscope.gaia.AsyncRpcChannelFetcher;
import com.alibaba.graphscope.gaia.GaiaGraphServer;
import com.alibaba.graphscope.gaia.MaxGraphStore;
import com.alibaba.graphscope.gaia.broadcast.AbstractBroadcastProcessor;
import com.alibaba.graphscope.gaia.broadcast.channel.RpcChannelFetcher;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.compiler.schema.JsonFileSchemaFetcher;
import com.alibaba.maxgraph.frontendservice.RemoteGraph;
import com.alibaba.maxgraph.frontendservice.server.ExecutorAddressFetcher;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Frontend extends com.alibaba.maxgraph.frontendservice.Frontend {
    private static final Logger logger = LoggerFactory.getLogger(Frontend.class);
    private GaiaGraphServer gaiaGraphServer;

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

        // add gaia compiler
        Configs configs = loadConfigs(this.instanceConfig);
        AsyncRpcChannelFetcher gaiaRpcFetcher = new AddressChannelFetcher(new ExecutorAddressFetcher(this.clientManager));
        GraphStoreService gaiaStoreService = new MaxGraphStore(schemaFetcher);
        AbstractBroadcastProcessor broadcastProcessor = new AsyncRpcBroadcastProcessor(gaiaRpcFetcher);
        gaiaGraphServer = new GaiaGraphServer(configs, gaiaStoreService, broadcastProcessor, new VineyardConfig(instanceConfig));

        gaiaGraphServer.start();
        this.gremlinServerPort = gaiaGraphServer.getGremlinServerPort();
    }

    @Override
    public void start() throws Exception {
        queryManager.start();
        startRpcService();
        startHBThread();
        this.gremlinExecutor = gaiaGraphServer.getGremlinExecutor();
    }

    private Configs loadConfigs(InstanceConfig instanceConfig) {
        logger.info("start to load configs");
        Properties properties = new Properties();
        instanceConfig.getAll().forEach((k, v) -> {
            if (v != null) {
                properties.setProperty(k, v);
            }
        });
        properties.setProperty(FrontendConfig.GREMLIN_PORT.getKey(), String.valueOf(instanceConfig.getGremlinServerPort()));
        logger.info("properties are {}", properties);
        return new Configs(properties);
    }
}
