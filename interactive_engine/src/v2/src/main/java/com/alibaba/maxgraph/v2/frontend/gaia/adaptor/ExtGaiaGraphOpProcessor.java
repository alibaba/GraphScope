package com.alibaba.maxgraph.v2.frontend.gaia.adaptor;

import com.alibaba.graphscope.gaia.broadcast.channel.RpcChannelFetcher;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.processor.GaiaGraphOpProcessor;
import com.alibaba.graphscope.gaia.store.GraphStoreService;

public class ExtGaiaGraphOpProcessor extends GaiaGraphOpProcessor {
    public ExtGaiaGraphOpProcessor(GaiaConfig config, GraphStoreService graphStore, RpcChannelFetcher fetcher) {
        super(config, graphStore, fetcher);
    }

    @Override
    public String getName() {
        return "gaia";
    }
}
