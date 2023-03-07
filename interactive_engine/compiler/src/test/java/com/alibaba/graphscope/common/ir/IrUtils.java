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
import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import java.net.URL;

public class IrUtils {
    public static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    public static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    public static final IrMeta schemaMeta = mockSchemaMeta();

    public static final GraphBuilder mockGraphBuilder() {
        GraphOptCluster cluster = GraphOptCluster.create(rexBuilder);
        return GraphBuilder.create(
                null, cluster, new GraphOptSchema(cluster, schemaMeta.getSchema()));
    }

    private static IrMeta mockSchemaMeta() {
        try {
            URL schemaResource =
                    Thread.currentThread()
                            .getContextClassLoader()
                            .getResource("schema/modern.json");
            Configs configs =
                    new Configs(
                            ImmutableMap.of(
                                    GraphConfig.GRAPH_SCHEMA.getKey(),
                                    schemaResource.toURI().getPath()));
            return new ExperimentalMetaFetcher(configs).fetch().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
