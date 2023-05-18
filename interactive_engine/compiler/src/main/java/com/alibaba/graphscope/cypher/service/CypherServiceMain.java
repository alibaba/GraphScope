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

package com.alibaba.graphscope.cypher.service;

import com.alibaba.graphscope.common.antlr4.Antlr4Parser;
import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.google.common.collect.ImmutableMap;
import org.neo4j.server.CommunityBootstrapper;

import java.nio.file.Path;

public class CypherServiceMain {
    public static void main(String[] args) throws Exception {
        Configs graphConfig = new Configs("conf/ir.compiler.properties");
        Antlr4Parser cypherParser = new CypherAntlr4Parser();
        GraphPlanner graphPlanner = new GraphPlanner(graphConfig);
        IrMetaQueryCallback queryCallback = new IrMetaQueryCallback(new ExperimentalMetaFetcher(graphConfig));
        ExecutionClient client = new RpcExecutionClient(graphConfig, new HostsRpcChannelFetcher(graphConfig));
        CommunityBootstrapper bootstrapper = new CypherBootstrapper(graphConfig, cypherParser, graphPlanner, queryCallback, client);
        bootstrapper.start(
                Path.of(
                        "/tmp/neo4j"),
                Path.of("neo4j.conf"),
                ImmutableMap.of(),
                false);
    }
}
