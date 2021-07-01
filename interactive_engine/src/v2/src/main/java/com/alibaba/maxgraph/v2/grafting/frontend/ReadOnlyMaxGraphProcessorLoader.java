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
package com.alibaba.maxgraph.v2.grafting.frontend;

import com.alibaba.graphscope.gaia.broadcast.channel.RpcChannelFetcher;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.maxgraph.v2.frontend.gaia.adaptor.VineyardGaiaConfig;
import com.alibaba.graphscope.gaia.processor.GaiaGraphOpProcessor;
import com.alibaba.graphscope.gaia.processor.TraversalOpProcessor;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.maxgraph.api.query.QueryCallbackManager;
import com.alibaba.maxgraph.api.query.QueryStatus;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.server.MaxGraphOpLoader;
import com.alibaba.maxgraph.server.processor.MixedOpProcessor;
import com.alibaba.maxgraph.server.processor.MixedTraversalOpProcessor;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.frontend.server.loader.ProcessorLoader;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

public class ReadOnlyMaxGraphProcessorLoader implements ProcessorLoader {

    private TinkerMaxGraph graph;
    private SchemaFetcher schemaFetcher;
    private RpcAddressFetcher rpcAddressFetcher;
    private InstanceConfig instanceConfig;
    private GraphStoreService gaiaStoreService;
    private RpcChannelFetcher gaiaRpcFetcher;

    public ReadOnlyMaxGraphProcessorLoader(Configs configs, TinkerMaxGraph graph, SchemaFetcher schemaFetcher,
                                           RpcAddressFetcher rpcAddressFetcher, RpcChannelFetcher gaiaRpcFetcher,
                                           GraphStoreService gaiaStoreService) {
        this.graph = graph;
        this.schemaFetcher = schemaFetcher;
        this.rpcAddressFetcher = rpcAddressFetcher;
        this.instanceConfig = new InstanceConfig(configs.getInnerProperties());
        CostDataStatistics.initialize(schemaFetcher);
        this.gaiaStoreService = gaiaStoreService;
        this.gaiaRpcFetcher = gaiaRpcFetcher;
    }

    @Override
    public void loadProcessor(Settings settings) {
        OpLoader.init(settings);
        QueryCallbackManager queryCallbackManager = new QueryCallbackManager() {
            @Override
            public QueryStatus beforeExecution(Long snapshotId) {
                return null;
            }

            @Override
            public void afterExecution(QueryStatus query) {

            }
        };
        MixedOpProcessor mixedOpProcessor = new MixedOpProcessor(this.graph,
                this.instanceConfig,
                this.rpcAddressFetcher,
                this.schemaFetcher,
                null,
                null,
                queryCallbackManager);
        mixedOpProcessor.init(settings);
        MaxGraphOpLoader.addOpProcessor(mixedOpProcessor.getName(), mixedOpProcessor);

        MixedTraversalOpProcessor mixedTraversalOpProcessor = new MixedTraversalOpProcessor(this.graph,
                this.instanceConfig,
                this.rpcAddressFetcher,
                this.schemaFetcher,
                null,
                queryCallbackManager);
        mixedTraversalOpProcessor.init(settings);
        MaxGraphOpLoader.addOpProcessor(mixedTraversalOpProcessor.getName(), mixedTraversalOpProcessor);

        // add gaia compiler
        GaiaConfig gaiaConfig = new VineyardGaiaConfig(this.instanceConfig);
        GaiaGraphOpProcessor gaiaGraphOpProcessor = new GaiaGraphOpProcessor(gaiaConfig, this.gaiaStoreService, this.gaiaRpcFetcher);
        MaxGraphOpLoader.addOpProcessor("gaia", gaiaGraphOpProcessor);

        // add gaia traversal compiler
        TraversalOpProcessor traversalOpProcessor = new TraversalOpProcessor(gaiaConfig, this.gaiaStoreService, this.gaiaRpcFetcher);
        MaxGraphOpLoader.addOpProcessor(traversalOpProcessor.getName(), traversalOpProcessor);
    }
}
