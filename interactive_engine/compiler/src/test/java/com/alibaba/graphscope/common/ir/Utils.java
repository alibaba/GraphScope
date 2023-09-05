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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URL;

public class Utils {
    public static final RelDataTypeFactory typeFactory = new GraphTypeFactoryImpl();
    public static final RexBuilder rexBuilder = new GraphRexBuilder(typeFactory);
    public static final IrMeta schemaMeta = mockSchemaMeta();
    public static final RelBuilderFactory relBuilderFactory =
            (RelOptCluster cluster, @Nullable RelOptSchema schema) ->
                    GraphBuilder.create(null, (GraphOptCluster) cluster, schema);

    public static final GraphBuilder mockGraphBuilder() {
        GraphOptCluster cluster = GraphOptCluster.create(mockPlanner(), rexBuilder);
        return GraphBuilder.create(
                null, cluster, new GraphOptSchema(cluster, schemaMeta.getSchema()));
    }

    public static final RelOptPlanner mockPlanner(RelRule.Config... rules) {
        HepProgramBuilder hepBuilder = HepProgram.builder();
        if (rules.length > 0) {
            for (RelRule.Config ruleConfig : rules) {
                hepBuilder.addRuleInstance(
                        ruleConfig.withRelBuilderFactory(relBuilderFactory).toRule());
            }
        }
        return new HepPlanner(hepBuilder.build());
    }

    private static IrMeta mockSchemaMeta() {
        try {
            URL schemaResource =
                    Thread.currentThread()
                            .getContextClassLoader()
                            .getResource("schema/modern.json");
            URL proceduresResource =
                    Thread.currentThread()
                            .getContextClassLoader()
                            .getResource("config/modern/plugins");
            Configs configs =
                    new Configs(
                            ImmutableMap.of(
                                    GraphConfig.GRAPH_SCHEMA.getKey(),
                                    schemaResource.toURI().getPath(),
                                    GraphConfig.GRAPH_STORED_PROCEDURES.getKey(),
                                    proceduresResource.toURI().getPath()));
            return new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
