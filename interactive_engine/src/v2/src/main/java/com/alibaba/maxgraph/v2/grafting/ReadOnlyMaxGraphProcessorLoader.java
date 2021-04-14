package com.alibaba.maxgraph.v2.grafting;

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

    public ReadOnlyMaxGraphProcessorLoader(Configs configs, TinkerMaxGraph graph, SchemaFetcher schemaFetcher,
                                           RpcAddressFetcher rpcAddressFetcher) {
        this.graph = graph;
        this.schemaFetcher = schemaFetcher;
        this.rpcAddressFetcher = rpcAddressFetcher;
        this.instanceConfig = new InstanceConfig(configs.getInnerProperties());
        CostDataStatistics.initialize(schemaFetcher);
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
    }
}
