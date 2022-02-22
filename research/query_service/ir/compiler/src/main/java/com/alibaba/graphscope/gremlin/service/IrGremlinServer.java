package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.store.StoreConfigs;
import com.alibaba.graphscope.gremlin.integration.processor.IrTestOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.processor.IrOpLoader;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;

import java.util.Map;

public class IrGremlinServer implements AutoCloseable {
    private static final IrCoreLibrary INSTANCE = IrCoreLibrary.INSTANCE;

    private GremlinServer gremlinServer;

    public void start(Configs configs, StoreConfigs storeConfigs, RpcChannelFetcher fetcher) throws Exception {
        AbstractOpProcessor standardProcessor = new IrStandardOpProcessor(configs, fetcher);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor = new IrTestOpProcessor(configs, fetcher);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);

        // pass store configs to ir_core
        Map<String, Object> configMap = storeConfigs.getConfigs();
        INSTANCE.setSchema((String) configMap.get("graph.schema"));

        Settings settings = loadSettings();
        this.gremlinServer = new GremlinServer(settings);
        this.gremlinServer.start();
    }

    @Override
    public void close() throws Exception {
        if (this.gremlinServer != null) {
            this.gremlinServer.stop();
        }
    }

    private Settings loadSettings() throws Exception {
        return Settings.read("conf/gremlin-server.yaml");
    }
}
