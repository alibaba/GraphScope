package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.graphscope.gaia.broadcast.AsyncRpcBroadcastProcessor;
import com.alibaba.graphscope.gaia.broadcast.channel.AsyncRpcChannelFetcher;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.groot.discovery.DiscoveryFactory;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.frontend.WriteSessionGenerator;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.servers.AbstractService;
import com.alibaba.maxgraph.servers.ComputeServiceProducer;

public class GaiaServiceProducer implements ComputeServiceProducer {

    private Configs configs;

    public GaiaServiceProducer(Configs configs) {
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
        return makeGraphService(schemaFetcher, channelManager);
    }

    @Override
    public AbstractService makeGraphService(
            SchemaFetcher schemaFetcher, ChannelManager channelManager) {
        int executorCount = CommonConfig.STORE_NODE_COUNT.get(this.configs);
        AsyncRpcChannelFetcher gaiaRpcFetcher =
                new ChannelManagerFetcher(channelManager, executorCount, RoleType.GAIA_RPC);
        GraphStoreService gaiaStoreService = new MaxGraphStore(schemaFetcher);
        return new GaiaGraphServer(
                configs,
                gaiaStoreService,
                new AsyncRpcBroadcastProcessor(gaiaRpcFetcher),
                new MaxGraphConfig(configs));
    }

    @Override
    public AbstractService makeExecutorService(
            StoreService storeService, MetaService metaService, DiscoveryFactory discoveryFactory) {
        ExecutorEngine executorEngine = new GaiaEngine(configs, discoveryFactory);
        return new GaiaService(configs, executorEngine, storeService, metaService);
    }
}
