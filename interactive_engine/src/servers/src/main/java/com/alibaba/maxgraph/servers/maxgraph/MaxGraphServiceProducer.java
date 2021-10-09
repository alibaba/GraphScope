package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.graphscope.groot.discovery.DiscoveryFactory;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.frontend.WriteSessionGenerator;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.servers.AbstractService;
import com.alibaba.maxgraph.servers.ComputeServiceProducer;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;

public class MaxGraphServiceProducer implements ComputeServiceProducer {

    private Configs configs;

    public MaxGraphServiceProducer(Configs configs) {
        this.configs = configs;
    }

    @Override
    public AbstractService makeGraphService(
            SchemaFetcher schemaFetcher,
            ChannelManager channelManager,
            NodeDiscovery discovery,
            GraphWriter graphWriter,
            WriteSessionGenerator writeSessionGenerator,
            MetaService metaService) {
        MaxGraphImpl maxGraphImpl =
                new MaxGraphImpl(
                        discovery, schemaFetcher, graphWriter, writeSessionGenerator, metaService);
        TinkerMaxGraph graph =
                new TinkerMaxGraph(
                        new InstanceConfig(configs.getInnerProperties()),
                        maxGraphImpl,
                        new DefaultGraphDfs());
        return new ReadOnlyGraphServer(
                configs, graph, schemaFetcher, new DiscoveryAddressFetcher(discovery));
    }

    @Override
    public AbstractService makeGraphService(
            SchemaFetcher schemaFetcher, ChannelManager channelManager) {
        throw new UnsupportedOperationException("maxgraph engine needs more parameters");
    }

    @Override
    public AbstractService makeExecutorService(
            StoreService storeService, MetaService metaService, DiscoveryFactory discoveryFactory) {
        return new ExecutorService(configs, storeService, discoveryFactory, metaService);
    }
}
