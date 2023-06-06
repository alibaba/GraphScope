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

import com.alibaba.graphscope.common.antlr4.Antlr4Parser;
import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.HttpExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
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
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.service.CypherBootstrapper;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;
import com.google.common.collect.ImmutableMap;

import org.neo4j.server.CommunityBootstrapper;

import java.nio.file.Path;

public class GraphServiceMain {
    public static final String EXPERIMENTAL = "exp";
    public static final String CSR = "csr";

    public static void main(String[] args) throws Exception {
        Configs configs = new Configs("conf/ir.compiler.properties", FileLoadType.RELATIVE_PATH);
        IrMetaQueryCallback queryCallback =
                new IrMetaQueryCallback(new ExperimentalMetaFetcher(configs));
        startGremlinService(configs, queryCallback);
        startCypherService(configs, queryCallback);
    }

    private static void startGremlinService(Configs configs, IrMetaQueryCallback queryCallback)
            throws Exception {
        ChannelFetcher channelFetcher = new HostsRpcChannelFetcher(configs);
        IrGremlinServer server = new IrGremlinServer();
        String storeType = GraphConfig.GRAPH_STORE.get(configs);
        if (storeType.equals(EXPERIMENTAL)) {
            server.start(configs, channelFetcher, queryCallback, TestGraphFactory.EXPERIMENTAL);
        } else if (storeType.equals(CSR)) {
            server.start(configs, channelFetcher, queryCallback, TestGraphFactory.MCSR);
        } else {
            throw new IllegalArgumentException("storage type " + storeType + " is invalid");
        }
    }

    private static void startCypherService(Configs configs, IrMetaQueryCallback queryCallback) {
        Antlr4Parser cypherParser = new CypherAntlr4Parser();
        GraphPlanner graphPlanner = new GraphPlanner(configs);
        ExecutionClient client =
                FrontendConfig.ENGINE_TYPE.get(configs).equals("pegasus")
                        ? new RpcExecutionClient(configs, new HostsRpcChannelFetcher(configs))
                        : new HttpExecutionClient(configs, new HostURIChannelFetcher(configs));
        CommunityBootstrapper bootstrapper =
                new CypherBootstrapper(configs, cypherParser, graphPlanner, queryCallback, client);
        bootstrapper.start(
                Path.of("/tmp/neo4j"), Path.of("conf/neo4j.conf"), ImmutableMap.of(), false);
    }
}
