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
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.tools.*;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.alibaba.graphscope.cypher.service.CypherBootstrapper;
import com.alibaba.graphscope.gremlin.antlr4x.parser.GremlinAntlr4Parser;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.GraphBuilderVisitor;
import com.alibaba.graphscope.gremlin.integration.result.GraphProperties;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import org.apache.commons.io.FileUtils;
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
    private final Configs configs;
    private final ChannelFetcher channelFetcher;
    private final IrMetaQueryCallback metaQueryCallback;
    private final GraphProperties testGraph;

    private IrGremlinServer gremlinServer;
    private CypherBootstrapper cypherBootstrapper;

    public GraphServer(
            Configs configs,
            ChannelFetcher channelFetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph) {
        this.configs = configs;
        this.channelFetcher = channelFetcher;
        this.metaQueryCallback = metaQueryCallback;
        this.testGraph = testGraph;
    }

    public void start() throws Exception {
        ExecutionClient executionClient = ExecutionClient.Factory.create(configs, channelFetcher);
        QueryIdGenerator idGenerator = new QueryIdGenerator(configs);
        if (!FrontendConfig.GREMLIN_SERVER_DISABLED.get(configs)) {
            GraphPlanner graphPlanner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String query) ->
                                    new LogicalPlan(
                                            new GraphBuilderVisitor(builder)
                                                    .visit(new GremlinAntlr4Parser().parse(query))
                                                    .build()));
            QueryCache queryCache = new QueryCache(configs, graphPlanner);
            this.gremlinServer =
                    new IrGremlinServer(
                            configs,
                            idGenerator,
                            queryCache,
                            executionClient,
                            channelFetcher,
                            metaQueryCallback,
                            testGraph);
            this.gremlinServer.start();
        }
        if (!FrontendConfig.NEO4J_BOLT_SERVER_DISABLED.get(configs)) {
            GraphPlanner graphPlanner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String query) ->
                                    new LogicalPlanVisitor(builder, irMeta)
                                            .visit(new CypherAntlr4Parser().parse(query)));
            QueryCache queryCache = new QueryCache(configs, graphPlanner);
            this.cypherBootstrapper =
                    new CypherBootstrapper(
                            configs, idGenerator, metaQueryCallback, executionClient, queryCache);
            Path neo4jHomePath = getNeo4jHomePath();
            this.cypherBootstrapper.start(
                    neo4jHomePath,
                    getNeo4jConfPath(neo4jHomePath),
                    ImmutableMap.of(
                            "dbms.connector.bolt.listen_address",
                            ":" + FrontendConfig.NEO4J_BOLT_SERVER_PORT.get(this.configs),
                            "dbms.connector.bolt.advertised_address",
                            ":" + FrontendConfig.NEO4J_BOLT_SERVER_PORT.get(this.configs),
                            "dbms.transaction.timeout",
                            FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(this.configs) + "ms"),
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
        if (!FrontendConfig.GREMLIN_SERVER_DISABLED.get(configs) && this.gremlinServer != null) {
            this.gremlinServer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].isEmpty()) {
            throw new IllegalArgumentException("usage: GraphServer '<path_to_config_file>'");
        }
        Configs configs = Configs.Factory.create(args[0]);
        IrMetaQueryCallback queryCallback =
                new IrMetaQueryCallback(
                        new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)));
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
