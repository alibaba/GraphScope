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

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;

import java.util.List;
import java.util.function.Function;

public class CypherDatabaseManagementServiceFactory extends DatabaseManagementServiceFactory {
    private final List<Class<?>> classTypes;
    public CypherDatabaseManagementServiceFactory(DbmsInfo dbmsInfo, Function<GlobalModule, AbstractEditionModule> editionFactory, List<Class<?>> classTypes) {
        super(dbmsInfo, editionFactory);
        this.classTypes = classTypes;
    }

    @Override
    protected GlobalModule createGlobalModule(Config config, ExternalDependencies dependencies) {
        GlobalModule globalModule = new GlobalModule(config, this.dbmsInfo, dependencies);
        DependencyResolver dependencyResolver = dependencies.dependencies();
        DependencySatisfier dependencySatisfier = globalModule.getGlobalDependencies();
        for (Class<?> classType : classTypes) {
            if (dependencyResolver.containsDependency(classType)) {
                dependencySatisfier.satisfyDependency(dependencyResolver.resolveDependency(classType));
            }
        }
        return globalModule;
    }
}
