package com.alibaba.graphscope.gaia.processor;

import com.alibaba.graphscope.gaia.broadcast.channel.HostsChannelFetcher;
import com.alibaba.graphscope.gaia.broadcast.channel.RpcChannelFetcher;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

import java.util.HashMap;
import java.util.Map;

public final class GaiaProcessorLoader {
    public static void load(GaiaConfig config, GraphStoreService storeService) {
        try {
            Map<String, OpProcessor> gaiaProcessors = new HashMap<>();
            RpcChannelFetcher fetcher = new HostsChannelFetcher(config);
            gaiaProcessors.put("", new GaiaGraphOpProcessor(config, storeService, fetcher));
            gaiaProcessors.put("plan", new LogicPlanProcessor(config, storeService));
            gaiaProcessors.put("traversal", new TraversalOpProcessor(config, storeService, fetcher));
            PlanUtils.setFinalStaticField(OpLoader.class, "processors", gaiaProcessors);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
