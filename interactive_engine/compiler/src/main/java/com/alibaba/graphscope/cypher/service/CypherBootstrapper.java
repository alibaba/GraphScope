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
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.server.CommunityBootstrapper;

import java.util.Arrays;
import java.util.List;

public class CypherBootstrapper extends CommunityBootstrapper {
    private final Dependencies externalDependencies;
    private final List<Class<?>> externalClassTypes;

    public CypherBootstrapper(Configs graphConfig, Antlr4Parser cypherParser, GraphPlanner graphPlanner, IrMetaQueryCallback queryCallback, ExecutionClient client) {
        this.externalDependencies = createExternalDependencies(graphConfig, cypherParser, graphPlanner, queryCallback, client);
        this.externalClassTypes = Arrays.asList(Configs.class, Antlr4Parser.class, GraphPlanner.class, IrMetaQueryCallback.class, ExecutionClient.class);
    }

    @Override
    protected DatabaseManagementService createNeo(
            Config config, GraphDatabaseDependencies dependencies) {
        dependencies.dependencies(externalDependencies);
        DatabaseManagementServiceFactory facadeFactory =
                new CypherDatabaseManagementServiceFactory(
                        DbmsInfo.COMMUNITY, CypherModuleManagement::new, this.externalClassTypes);
        return facadeFactory.build(config, dependencies);
    }

    public Dependencies createExternalDependencies(Configs configs, Antlr4Parser cypherParser, GraphPlanner graphPlanner, IrMetaQueryCallback queryCallback, ExecutionClient client) {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies(configs, cypherParser, graphPlanner, queryCallback, client);
        return dependencies;
    }
}
