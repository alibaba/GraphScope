package com.alibaba.graphscope.groot.servers;

import com.alibaba.graphscope.groot.common.schema.api.SchemaFetcher;
import com.alibaba.graphscope.groot.discovery.DiscoveryFactory;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.store.StoreService;

public interface ComputeServiceProducer {
    AbstractService makeGraphService(SchemaFetcher schemaFetcher, ChannelManager channelManager);

    AbstractService makeExecutorService(
            StoreService storeService, MetaService metaService, DiscoveryFactory discoveryFactory);
}
