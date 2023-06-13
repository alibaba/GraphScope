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
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.gremlin.Utils;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.server.CommunityBootstrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class CypherBootstrapper extends CommunityBootstrapper {
    private static final Logger logger = LoggerFactory.getLogger(CypherBootstrapper.class);
    private final Dependencies externalDependencies;
    private final List<Class<?>> externalClassTypes;
    private final ExecutionClient client;

    public CypherBootstrapper(
            Configs graphConfig,
            GraphPlanner graphPlanner,
            IrMetaQueryCallback queryCallback,
            ExecutionClient client) {
        this.client = client;
        this.externalDependencies =
                createExternalDependencies(
                        graphConfig, new CypherAntlr4Parser(), graphPlanner, queryCallback, client);
        this.externalClassTypes =
                Arrays.asList(
                        Configs.class,
                        Antlr4Parser.class,
                        GraphPlanner.class,
                        IrMetaQueryCallback.class,
                        ExecutionClient.class);
    }

    @Override
    protected DatabaseManagementService createNeo(
            Config config, GraphDatabaseDependencies dependencies) {
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        client.close();
                                    } catch (Exception e) {
                                        logger.error("close client error", e);
                                    }
                                }));
        dependencies.dependencies(externalDependencies);
        DatabaseManagementServiceFactory facadeFactory =
                new CypherDatabaseManagementServiceFactory(
                        DbmsInfo.COMMUNITY, CypherModuleManagement::new, this.externalClassTypes);
        return facadeFactory.build(config, dependencies);
    }

    private Dependencies createExternalDependencies(
            Configs configs,
            Antlr4Parser cypherParser,
            GraphPlanner graphPlanner,
            IrMetaQueryCallback queryCallback,
            ExecutionClient client) {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies(
                configs, cypherParser, graphPlanner, queryCallback, client);
        return dependencies;
    }

    private class CypherDatabaseManagementServiceFactory extends DatabaseManagementServiceFactory {
        private final List<Class<?>> externalClassTypes;

        public CypherDatabaseManagementServiceFactory(
                DbmsInfo dbmsInfo,
                Function<GlobalModule, AbstractEditionModule> editionFactory,
                List<Class<?>> externalClassTypes) {
            super(dbmsInfo, editionFactory);
            this.externalClassTypes = externalClassTypes;
        }

        @Override
        protected GlobalModule createGlobalModule(
                Config config, ExternalDependencies externalDependencies) {
            GlobalModule globalModule =
                    new GlobalModule(config, this.dbmsInfo, externalDependencies);
            DependencyResolver externalDependencyResolver =
                    globalModule.getExternalDependencyResolver();
            DependencySatisfier globalDependencySatisfier = globalModule.getGlobalDependencies();
            for (Class<?> classType : externalClassTypes) {
                if (externalDependencyResolver.containsDependency(classType)) {
                    globalDependencySatisfier.satisfyDependency(
                            externalDependencyResolver.resolveDependency(classType));
                }
            }
            return globalModule;
        }
    }

    private class CypherModuleManagement extends CommunityEditionModule {
        public CypherModuleManagement(GlobalModule globalModule) {
            super(globalModule);
            FabricServicesBootstrap bootstrap =
                    new CypherQueryServiceBootstrap(
                            globalModule.getGlobalLife(),
                            globalModule.getGlobalDependencies(),
                            globalModule.getLogService());
            Utils.setFieldValue(
                    CommunityEditionModule.class, this, "fabricServicesBootstrap", bootstrap);
        }
    }
}
