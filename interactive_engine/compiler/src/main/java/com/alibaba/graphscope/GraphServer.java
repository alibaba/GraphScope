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
import com.google.common.io.Resources;

import org.apache.commons.io.FileUtils;
import org.neo4j.server.CommunityBootstrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GraphServer {
    private static final Logger logger = LoggerFactory.getLogger(GraphServer.class);
    private final IrGremlinServer gremlinServer;
    private final CommunityBootstrapper cypherBootstrapper;
    private final Configs configs;

    public GraphServer(
            Configs configs,
            ChannelFetcher channelFetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph) {
        ExecutionClient executionClient = ExecutionClient.Factory.create(configs, channelFetcher);
        GraphPlanner graphPlanner = new GraphPlanner(configs);
        this.gremlinServer =
                new IrGremlinServer(
                        configs, graphPlanner, channelFetcher, metaQueryCallback, testGraph);
        this.cypherBootstrapper =
                new CypherBootstrapper(configs, graphPlanner, metaQueryCallback, executionClient);
        this.configs = configs;
    }

    public void start() throws Exception {
        if (!FrontendConfig.GREMLIN_SERVER_DISABLED.get(configs)) {
            this.gremlinServer.start();
        }
        if (!FrontendConfig.NEO4J_BOLT_SERVER_DISABLED.get(configs)) {
            Path neo4jHomePath = getNeo4jHomePath();
            this.cypherBootstrapper.start(
                    neo4jHomePath,
                    getNeo4jConfPath(neo4jHomePath),
                    ImmutableMap.of(
                            "dbms.connector.bolt.listen_address",
                            ":" + FrontendConfig.NEO4J_BOLT_SERVER_PORT.get(this.configs),
                            "dbms.connector.bolt.advertised_address",
                            ":" + FrontendConfig.NEO4J_BOLT_SERVER_PORT.get(this.configs)),
                    false);
        }
    }

    private Path getNeo4jHomePath() throws IOException {
        Path neo4jHomePath = Files.createTempDirectory("neo4j-");
        deleteOnExit(neo4jHomePath);
        return neo4jHomePath;
    }

    private void deleteOnExit(Path tmpPath) {
        // Register a shutdown hook to delete the temporary directory on exit
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        FileUtils.deleteDirectory(tmpPath.toFile());
                                    } catch (IOException e) {
                                        logger.error(
                                                "Failed to delete temporary directory {}",
                                                tmpPath,
                                                e);
                                    }
                                }));
    }

    private Path getNeo4jConfPath(Path neo4jHomePath) throws IOException {
        URL resourceUrl = getClass().getClassLoader().getResource("conf/neo4j.conf");
        String neo4jConf = Resources.toString(resourceUrl, StandardCharsets.UTF_8).trim();
        File tmpFile = new File(Paths.get(neo4jHomePath.toString(), "neo4j.conf").toString());
        FileUtils.writeStringToFile(tmpFile, neo4jConf, StandardCharsets.UTF_8);
        return tmpFile.toPath();
    }

    public void close() throws Exception {
        if (this.gremlinServer != null && !FrontendConfig.GREMLIN_SERVER_DISABLED.get(configs)) {
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
