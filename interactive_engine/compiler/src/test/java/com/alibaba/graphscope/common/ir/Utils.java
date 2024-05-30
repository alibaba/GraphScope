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
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.planner.GraphHepPlanner;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilderFactory;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.yaml.snakeyaml.Yaml;

import java.net.URL;
import java.util.Map;

public class Utils {
    public static final Configs configs =
            new Configs(ImmutableMap.of(FrontendConfig.CALCITE_DEFAULT_CHARSET.getKey(), "UTF-8"));
    public static final RelDataTypeFactory typeFactory = new GraphTypeFactoryImpl(configs);
    public static final RexBuilder rexBuilder = new GraphRexBuilder(typeFactory);
    public static final IrMeta schemaMeta = mockSchemaMeta("schema/modern.json");
    public static final RelBuilderFactory relBuilderFactory = new GraphBuilderFactory(configs);

    public static final GraphBuilder mockGraphBuilder() {
        GraphOptCluster cluster = GraphOptCluster.create(mockPlanner(), rexBuilder);
        return GraphBuilder.create(
                configs, cluster, new GraphOptSchema(cluster, schemaMeta.getSchema()));
    }

    public static final GraphBuilder mockGraphBuilder(String schemaJson) {
        GraphOptCluster cluster = GraphOptCluster.create(mockPlanner(), rexBuilder);
        return GraphBuilder.create(
                configs,
                cluster,
                new GraphOptSchema(cluster, mockSchemaMeta(schemaJson).getSchema()));
    }

    public static final GraphBuilder mockGraphBuilder(GraphRelOptimizer optimizer, IrMeta irMeta) {
        RelOptCluster optCluster =
                GraphOptCluster.create(optimizer.getMatchPlanner(), Utils.rexBuilder);
        optCluster.setMetadataQuerySupplier(() -> optimizer.createMetaDataQuery(irMeta));
        return (GraphBuilder)
                relBuilderFactory.create(
                        optCluster, new GraphOptSchema(optCluster, irMeta.getSchema()));
    }

    public static final GraphBuilder mockGraphBuilder(Configs configs) {
        GraphOptCluster cluster = GraphOptCluster.create(mockPlanner(), rexBuilder);
        return GraphBuilder.create(
                configs, cluster, new GraphOptSchema(cluster, schemaMeta.getSchema()));
    }

    public static final RelOptPlanner mockPlanner(RelRule.Config... rules) {
        HepProgramBuilder hepBuilder = HepProgram.builder();
        if (rules.length > 0) {
            for (RelRule.Config ruleConfig : rules) {
                hepBuilder.addRuleInstance(
                        ruleConfig.withRelBuilderFactory(relBuilderFactory).toRule());
            }
        }
        return new GraphHepPlanner(hepBuilder.build());
    }

    public static IrMeta mockSchemaMeta(String schemaJson) {
        try {
            URL schemaResource =
                    Thread.currentThread().getContextClassLoader().getResource(schemaJson);
            URL statisticsResource =
                    Thread.currentThread()
                            .getContextClassLoader()
                            .getResource("statistics/modern_statistics.json");
            String yamlStr = FileUtils.readJsonFromResource("config/modern/graph.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> yamlMap = yaml.load(yamlStr);
            String proceduresYaml = yaml.dump(yamlMap.get("stored_procedures"));
            Configs configs =
                    new Configs(
                            ImmutableMap.of(
                                    GraphConfig.GRAPH_SCHEMA.getKey(),
                                    schemaResource.toURI().getPath(),
                                    GraphConfig.GRAPH_STATISTICS.getKey(),
                                    statisticsResource.toURI().getPath(),
                                    GraphConfig.GRAPH_STORED_PROCEDURES_YAML.getKey(),
                                    proceduresYaml));
            return new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static IrMeta mockIrMeta(String schemaJson, String statisticsJson) {
        try {
            URL schemaResource =
                    Thread.currentThread().getContextClassLoader().getResource(schemaJson);
            URL statisticsResource =
                    Thread.currentThread().getContextClassLoader().getResource(statisticsJson);
            String yamlStr = FileUtils.readJsonFromResource("config/modern/graph.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> yamlMap = yaml.load(yamlStr);
            String proceduresYaml = yaml.dump(yamlMap.get("stored_procedures"));
            Configs configs =
                    new Configs(
                            ImmutableMap.of(
                                    GraphConfig.GRAPH_SCHEMA.getKey(),
                                    schemaResource.toURI().getPath(),
                                    GraphConfig.GRAPH_STATISTICS.getKey(),
                                    statisticsResource.toURI().getPath(),
                                    GraphConfig.GRAPH_STORED_PROCEDURES_YAML.getKey(),
                                    proceduresYaml));
            return new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
