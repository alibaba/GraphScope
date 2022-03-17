package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.processor.IrTestOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.processor.IrOpLoader;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;

import java.io.InputStream;

public class IrGremlinServer implements AutoCloseable {
    private GremlinServer gremlinServer;
    private Settings settings;

    public IrGremlinServer() {
        InputStream input = getClass().getClassLoader().getResourceAsStream("conf/gremlin-server.yaml");
        settings = Settings.read(input);
    }

    public IrGremlinServer(int gremlinPort) {
        this();
        settings.port = (gremlinPort >= 0) ? gremlinPort : settings.port;
    }

    public void start(Configs configs, IrMetaFetcher irMetaFetcher, RpcChannelFetcher fetcher) throws Exception {
        AbstractOpProcessor standardProcessor = new IrStandardOpProcessor(configs, irMetaFetcher, fetcher);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor = new IrTestOpProcessor(configs, irMetaFetcher, fetcher);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);

        this.gremlinServer = new GremlinServer(settings);
        this.gremlinServer.start();
    }

    @Override
    public void close() throws Exception {
        if (this.gremlinServer != null) {
            this.gremlinServer.stop();
        }
    }
}
