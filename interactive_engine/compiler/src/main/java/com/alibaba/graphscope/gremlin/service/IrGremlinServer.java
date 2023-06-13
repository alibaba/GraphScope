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
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.io.InputStream;

public class IrGremlinServer implements AutoCloseable {
    private final Configs configs;
    private final GraphPlanner graphPlanner;
    private final ChannelFetcher channelFetcher;
    private final IrMetaQueryCallback metaQueryCallback;
    private final GraphProperties testGraph;

    private GremlinServer gremlinServer;
    private final Settings settings;
    private final Graph graph;
    private final GraphTraversalSource g;

    public IrGremlinServer(
            Configs configs,
            GraphPlanner graphPlanner,
            ChannelFetcher channelFetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph) {
        this.configs = configs;
        this.graphPlanner = graphPlanner;
        this.channelFetcher = channelFetcher;
        this.metaQueryCallback = metaQueryCallback;
        this.testGraph = testGraph;
        InputStream input =
                getClass().getClassLoader().getResourceAsStream("conf/gremlin-server.yaml");
        this.settings = Settings.read(input);
        this.settings.host = "0.0.0.0";
        int port = FrontendConfig.FRONTEND_SERVICE_PORT.get(configs);
        if (port >= 0) {
            this.settings.port = port;
        }
        this.graph = TinkerFactory.createModern();
        this.g = this.graph.traversal(IrCustomizedTraversalSource.class);
    }

    public void start() throws Exception {
        AbstractOpProcessor standardProcessor =
                new IrStandardOpProcessor(
                        configs, graphPlanner, channelFetcher, metaQueryCallback, graph, g);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor =
                new IrTestOpProcessor(
                        configs,
                        graphPlanner,
                        channelFetcher,
                        metaQueryCallback,
                        graph,
                        g,
                        testGraph);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);

        AuthManager authManager = new DefaultAuthManager(configs);
        AuthManagerReference.setAuthManager(authManager);

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
}
