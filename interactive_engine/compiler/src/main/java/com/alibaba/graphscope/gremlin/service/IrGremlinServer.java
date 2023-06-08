/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.auth.AuthManager;
import com.alibaba.graphscope.gremlin.auth.AuthManagerReference;
import com.alibaba.graphscope.gremlin.auth.DefaultAuthManager;
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
    private final Graph graph;
    private final GraphTraversalSource g;
    private final Configs configs;

    public IrGremlinServer(
            Configs configs,
            ChannelFetcher channelFetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph) {
        this.configs = configs;
        this.graph = TinkerFactory.createModern();
        this.g = this.graph.traversal(IrCustomizedTraversalSource.class);
        initProcessor(channelFetcher, metaQueryCallback, testGraph);
        initAuthentication();
        initGremlinServer();
    }

    private void initProcessor(
            ChannelFetcher channelFetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph) {
        AbstractOpProcessor standardProcessor =
                new IrStandardOpProcessor(configs, channelFetcher, metaQueryCallback, graph, g);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor =
                new IrTestOpProcessor(
                        configs, channelFetcher, metaQueryCallback, graph, g, testGraph);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);
    }

    private void initAuthentication() {
        AuthManager authManager = new DefaultAuthManager(configs);
        AuthManagerReference.setAuthManager(authManager);
    }

    private void initGremlinServer() {
        InputStream input =
                getClass().getClassLoader().getResourceAsStream("conf/gremlin-server.yaml");
        Settings settings = Settings.read(input);
        settings.host = "0.0.0.0";
        int port = FrontendConfig.FRONTEND_SERVICE_PORT.get(configs);
        if (port >= 0) {
            settings.port = port;
        }
        this.gremlinServer = new GremlinServer(settings);
        ServerGremlinExecutor serverGremlinExecutor =
                Utils.getFieldValue(
                        GremlinServer.class, this.gremlinServer, "serverGremlinExecutor");
        serverGremlinExecutor.getGraphManager().putGraph("graph", graph);
        serverGremlinExecutor.getGraphManager().putTraversalSource("g", graph.traversal());
    }

    public void start() throws Exception {
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
