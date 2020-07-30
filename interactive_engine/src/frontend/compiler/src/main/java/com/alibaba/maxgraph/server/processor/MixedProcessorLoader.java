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
package com.alibaba.maxgraph.server.processor;

import com.alibaba.maxgraph.api.manager.RecordProcessorManager;
import com.alibaba.maxgraph.api.query.QueryCallbackManager;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.compiler.prepare.store.StatementStore;
import com.alibaba.maxgraph.server.MaxGraphOpLoader;
import com.alibaba.maxgraph.server.ProcessorLoader;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

public class MixedProcessorLoader implements ProcessorLoader {
    private TinkerMaxGraph graph;
    private InstanceConfig instanceConfig;
    private RpcAddressFetcher executorAddressFetcher;
    private SchemaFetcher schemaFetcher;
    private StatementStore statementStore;
    private RecordProcessorManager recordProcessorManager;
    private QueryCallbackManager queryCallbackManager;

    private MixedProcessorLoader(TinkerMaxGraph graph,
                                 InstanceConfig instanceConfig,
                                 RpcAddressFetcher executorAddressFetcher,
                                 SchemaFetcher schemaFetcher,
                                 StatementStore statementStore,
                                 RecordProcessorManager recordProcessorManager,
                                 QueryCallbackManager queryCallbackManager) {
        this.graph = graph;
        this.instanceConfig = instanceConfig;
        this.executorAddressFetcher = executorAddressFetcher;
        this.schemaFetcher = schemaFetcher;
        this.statementStore = statementStore;
        this.recordProcessorManager = recordProcessorManager;
        this.queryCallbackManager = queryCallbackManager;
    }

    @Override
    public void loadProcessor(Settings settings) throws Exception {
        OpLoader.init(settings);

        MixedOpProcessor mixedOpProcessor = new MixedOpProcessor(
                graph,
                instanceConfig,
                executorAddressFetcher,
                schemaFetcher,
                statementStore,
                recordProcessorManager,
                queryCallbackManager);
        mixedOpProcessor.init(settings);
        // replace StandardOpProcessor
        MaxGraphOpLoader.addOpProcessor(mixedOpProcessor.getName(), mixedOpProcessor);

        MixedTraversalOpProcessor mixedTraversalOpProcessor = new MixedTraversalOpProcessor(
                graph,
                instanceConfig,
                executorAddressFetcher,
                schemaFetcher,
                statementStore,
                queryCallbackManager);
        mixedTraversalOpProcessor.init(settings);
        // replace TraversalOpProcessor
        MaxGraphOpLoader.addOpProcessor(mixedTraversalOpProcessor.getName(), mixedTraversalOpProcessor);
    }

    public static ProcessorLoader newProcessorLoader(TinkerMaxGraph graph,
                                                     InstanceConfig instanceConfig,
                                                     RpcAddressFetcher executorAddressFetcher,
                                                     SchemaFetcher schemaFetcher,
                                                     StatementStore statementStore,
                                                     RecordProcessorManager recordProcessorManager,
                                                     QueryCallbackManager queryCallbackManager) {
        CostDataStatistics.initialize(schemaFetcher);
        return new MixedProcessorLoader(graph,
                instanceConfig,
                executorAddressFetcher,
                schemaFetcher,
                statementStore,
                recordProcessorManager,
                queryCallbackManager);
    }
}
