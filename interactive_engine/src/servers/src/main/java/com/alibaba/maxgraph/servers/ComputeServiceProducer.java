package com.alibaba.maxgraph.servers;

import com.alibaba.graphscope.groot.discovery.DiscoveryFactory;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.frontend.WriteSessionGenerator;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;

public interface ComputeServiceProducer {
    // Used for compatibility of maxgraph engine
    @Deprecated
    AbstractService makeGraphService(
            SchemaFetcher schemaFetcher,
            ChannelManager channelManager,
            NodeDiscovery discovery,
            GraphWriter graphWriter,
            WriteSessionGenerator writeSessionGenerator,
            MetaService metaService);

    AbstractService makeGraphService(SchemaFetcher schemaFetcher, ChannelManager channelManager);

    AbstractService makeExecutorService(
            StoreService storeService, MetaService metaService, DiscoveryFactory discoveryFactory);
}
