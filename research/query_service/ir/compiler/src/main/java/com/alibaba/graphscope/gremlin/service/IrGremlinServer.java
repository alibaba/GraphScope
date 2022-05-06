package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.integration.processor.IrTestOpProcessor;
import com.alibaba.graphscope.gremlin.integration.result.GraphProperties;
import com.alibaba.graphscope.gremlin.plugin.processor.IrOpLoader;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;

import io.netty.channel.Channel;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class IrGremlinServer implements AutoCloseable {
    private GremlinServer gremlinServer;
    private Settings settings;
    private Graph graph;
    private GraphTraversalSource g;

    public IrGremlinServer() {
        InputStream input =
                getClass().getClassLoader().getResourceAsStream("conf/gremlin-server.yaml");
        this.settings = Settings.read(input);
        this.graph = TinkerFactory.createModern();
        this.g = this.graph.traversal(IrCustomizedTraversalSource.class);
    }

    public IrGremlinServer(int gremlinPort) {
        this();
        settings.port = (gremlinPort >= 0) ? gremlinPort : settings.port;
    }

    public void start(
            Configs configs,
            IrMetaFetcher irMetaFetcher,
            RpcChannelFetcher fetcher,
            GraphProperties testGraph)
            throws Exception {
        AbstractOpProcessor standardProcessor =
                new IrStandardOpProcessor(configs, irMetaFetcher, fetcher, graph, g);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor =
                new IrTestOpProcessor(configs, irMetaFetcher, fetcher, graph, g, testGraph);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);

        this.gremlinServer = new GremlinServer(settings);

        ServerGremlinExecutor serverGremlinExecutor =
                Utils.getFieldValue(
                        GremlinServer.class, this.gremlinServer, "serverGremlinExecutor");
        serverGremlinExecutor.getGraphManager().putGraph("graph", graph);
        serverGremlinExecutor.getGraphManager().putTraversalSource("g", graph.traversal());

        this.gremlinServer.start().join();
    }

    @Override
    public void close() throws Exception {
        if (this.gremlinServer != null) {
            this.gremlinServer.stop();
        }
    }

    public int getGremlinServerPort() throws Exception {
        Field ch = this.gremlinServer.getClass().getDeclaredField("ch");
        ch.setAccessible(true);
        Channel o = (Channel) ch.get(this.gremlinServer);
        SocketAddress localAddr = o.localAddress();
        return ((InetSocketAddress) localAddr).getPort();
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinServer.getServerGremlinExecutor().getGremlinExecutor();
    }
}
