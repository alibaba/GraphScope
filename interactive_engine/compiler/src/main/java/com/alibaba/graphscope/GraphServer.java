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

package com.alibaba.graphscope;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.channel.HostURIChannelFetcher;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FileLoadType;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.cypher.service.CypherBootstrapper;
import com.alibaba.graphscope.gremlin.integration.result.GraphProperties;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;
import com.google.common.collect.ImmutableMap;

import org.neo4j.server.CommunityBootstrapper;

import java.nio.file.Path;

public class GraphServer {
    private final IrGremlinServer gremlinServer;
    private final CommunityBootstrapper cypherBootstrapper;

    public GraphServer(
            Configs configs,
            ChannelFetcher channelFetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph) {
        ExecutionClient executionClient = ExecutionClient.Factory.create(configs, channelFetcher);
        GraphPlanner graphPlanner = new GraphPlanner(configs);
        this.gremlinServer =
                new IrGremlinServer(configs, null, channelFetcher, metaQueryCallback, testGraph);
        this.cypherBootstrapper =
                new CypherBootstrapper(configs, graphPlanner, metaQueryCallback, executionClient);
    }

    public void start() throws Exception {
        this.gremlinServer.start();
        this.cypherBootstrapper.start(
                Path.of("/tmp/neo4j"), Path.of("conf/neo4j.conf"), ImmutableMap.of(), false);
    }

    public void close() throws Exception {
        if (this.gremlinServer != null) {
            this.gremlinServer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configs configs = new Configs("conf/ir.compiler.properties", FileLoadType.RELATIVE_PATH);
        IrMetaQueryCallback queryCallback =
                new IrMetaQueryCallback(new ExperimentalMetaFetcher(configs));
        GraphServer server =
                new GraphServer(
                        configs, getChannelFetcher(configs), queryCallback, getTestGraph(configs));
        server.start();
    }

    private static GraphProperties getTestGraph(Configs configs) {
        GraphProperties testGraph;
        switch (GraphConfig.GRAPH_STORE.get(configs)) {
            case "exp":
                testGraph = TestGraphFactory.EXPERIMENTAL;
                break;
            case "csr":
                testGraph = TestGraphFactory.MCSR;
                break;
            default:
                throw new IllegalArgumentException("unknown graph store type");
        }
        return testGraph;
    }

    private static ChannelFetcher getChannelFetcher(Configs configs) {
        switch (FrontendConfig.ENGINE_TYPE.get(configs)) {
            case "pegasus":
                return new HostsRpcChannelFetcher(configs);
            case "hiactor":
                return new HostURIChannelFetcher(configs);
            default:
                throw new IllegalArgumentException("unknown engine type");
        }
    }
}
