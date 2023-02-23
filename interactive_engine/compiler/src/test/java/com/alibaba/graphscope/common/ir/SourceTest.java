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

import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.schema.GraphSchemaWrapper;
import com.alibaba.graphscope.common.ir.schema.StatisticSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.compiler.schema.DefaultGraphSchema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.junit.Assert;
import org.junit.Test;

public class SourceTest {
    private static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private static final StatisticSchema schema = new GraphSchemaWrapper(mockGraphSchema(), true);

    // g.V().hasLabel("person")
    @Test
    public void single_label_test() {
        GraphBuilder builder = mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RelNode source = builder.source(sourceConfig).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[~DEFAULT],"
                        + " opt=[VERTEX])",
                source.explain().trim());
    }

    // g.V().hasLabel("person", "software").as("a")
    @Test
    public void multiple_labels_test() {
        GraphBuilder builder = mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(
                        GraphOpt.Source.VERTEX,
                        new LabelConfig(false).addLabel("person").addLabel("software"),
                        "a");
        RelNode source = builder.source(sourceConfig).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person, software]}],"
                        + " alias=[a], opt=[VERTEX])",
                source.explain().trim());
    }

    @Test
    public void multiple_labels_opt_test() {
        try {
            GraphBuilder builder = mockGraphBuilder();
            SourceConfig sourceConfig =
                    new SourceConfig(
                            GraphOpt.Source.VERTEX,
                            new LabelConfig(false).addLabel("person").addLabel("knows"));
            builder.source(sourceConfig).build();
        } catch (IllegalArgumentException e) {
            // expected error
            return;
        }
        Assert.fail("person and knows have different opt types, should have thrown errors");
    }

    public static final GraphBuilder mockGraphBuilder() {
        GraphOptCluster cluster = GraphOptCluster.create(rexBuilder);
        return GraphBuilder.create(null, cluster, new GraphOptSchema(cluster, schema));
    }

    private static GraphSchema mockGraphSchema() {
        String inputJson = FileUtils.readJsonFromResource("schema/modern.json");
        GraphSchema graphSchema = DefaultGraphSchema.buildSchemaFromJson(inputJson);
        return graphSchema;
    }
}
