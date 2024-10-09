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

import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class FilterTest {
    // source([person]).filter("XXX") are fused
    @Test
    public void equal_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable(null, "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(equal).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[VERTEX])",
                filter.explain().trim());
    }

    // source([person]).as('x').filter("XXX") are fused
    @Test
    public void equal_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(
                        GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"), "x");
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable(null, "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(equal).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[VERTEX])",
                filter.explain().trim());
    }

    // source([person]).as('x').filter('x.age == 10') are fused
    @Test
    public void equal_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(
                        GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"), "x");
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable("x", "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(equal).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[VERTEX])",
                filter.explain().trim());
    }

    // g.V().hasLabel("person").where(expr("@.age > 10"))
    @Test
    public void greater_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexNode greater =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.variable(null, "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(greater).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " fusedFilter=[[>(_.age, 10)]], opt=[VERTEX])",
                filter.explain().trim());
    }

    // 20 > 10 -> always returns true, ignore the condition
    @Test
    public void greater_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexNode greater =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.literal(20),
                                builder.literal(10));
        RelNode filter = builder.filter(greater).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " opt=[VERTEX])",
                filter.explain().trim());
    }

    /**
     * 10 > 20 -> always returns false, create {@link LogicalValues} which carries all data types of the node before the filter
     */
    @Test
    public void greater_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexNode greater =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.literal(10),
                                builder.literal(20));
        // the node before the filter
        RelNode previous = builder.peek();
        RelNode filter = builder.filter(greater).build();
        Assert.assertEquals(filter.getClass(), LogicalValues.class);
        Assert.assertEquals(filter.getRowType(), previous.getRowType());
    }

    // test fuzzy conditions: g.V().has("age", gt(10))
    @Test
    public void greater_4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig = new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(true));
        RexNode greater =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.variable(null, "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(greater).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                        + " alias=[_], fusedFilter=[[>(_.age, 10)]], opt=[VERTEX])",
                filter.explain().trim());
    }

    // Match (:person {age > $age})
    @Test
    public void greater_5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexCall greater =
                (RexCall)
                        builder.source(sourceConfig)
                                .call(
                                        GraphStdOperatorTable.GREATER_THAN,
                                        builder.variable(null, "age"),
                                        ((GraphRexBuilder) builder.getRexBuilder())
                                                .makeGraphDynamicParam("age", 0));
        Assert.assertEquals(">(_.age, ?0)", greater.toString());
        Assert.assertEquals(
                SqlTypeName.INTEGER, greater.getOperands().get(1).getType().getSqlTypeName());
    }

    // g.V().hasLabel("person").where(expr("@.age > 20 and @.name == marko"))
    @Test
    public void and_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexNode condition1 =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.variable(null, "age"),
                                builder.literal(20));
        RexNode condition2 =
                builder.call(
                        GraphStdOperatorTable.EQUALS,
                        builder.variable(null, "name"),
                        builder.literal("marko"));
        RelNode filter = builder.filter(condition1, condition2).build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " fusedFilter=[[AND(>(_.age, 20), =(_.name, _UTF-8'marko'))]],"
                        + " opt=[VERTEX])",
                filter.explain().trim());
    }

    // g.V().hasLabel("person").where(expr("@.age > 20 and @.age < 30"))
    @Test
    public void and_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"));
        RexNode condition1 =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.variable(null, "age"),
                                builder.literal(20));
        RexNode condition2 =
                builder.call(
                        GraphStdOperatorTable.LESS_THAN,
                        builder.variable(null, "age"),
                        builder.literal(30));
        RelNode filter =
                builder.filter(builder.call(GraphStdOperatorTable.AND, condition1, condition2))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " fusedFilter=[[AND(>(_.age, 20), <(_.age, 30))]], opt=[VERTEX])",
                filter.explain().trim());
    }

    // g.V().hasLabel('person')
    @Test
    public void label_equals_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode node =
                builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, GraphProperty.LABEL_KEY),
                                        builder.literal("person")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " opt=[VERTEX])",
                node.explain().trim());
    }

    // g.V().hasLabel('XX'), 'XX' is not a valid label
    @Test
    public void label_equals_2_test() {
        try {
            GraphBuilder builder = Utils.mockGraphBuilder();
            RelNode node =
                    builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                            .filter(
                                    builder.call(
                                            GraphStdOperatorTable.EQUALS,
                                            builder.variable(null, GraphProperty.LABEL_KEY),
                                            builder.literal("XX")))
                            .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "cannot find common labels between values= [XX] and label="
                            + " [[VertexLabel(software), VertexLabel(person)]]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    // g.V().hasLabel('person').hasLabel('software')
    @Test
    public void label_equals_3_test() {
        try {
            GraphBuilder builder = Utils.mockGraphBuilder();
            RelNode node =
                    builder.source(
                                    new SourceConfig(
                                            GraphOpt.Source.VERTEX,
                                            new LabelConfig(false).addLabel("person")))
                            .filter(
                                    builder.call(
                                            GraphStdOperatorTable.EQUALS,
                                            builder.variable(null, GraphProperty.LABEL_KEY),
                                            builder.literal("software")))
                            .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "cannot find common labels between values= [software] and label="
                            + " [[VertexLabel(person)]]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    // g.V().has('age', 10).hasLabel('person')
    @Test
    public void label_equals_4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode node =
                builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, "age"),
                                        builder.literal(10)))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, GraphProperty.LABEL_KEY),
                                        builder.literal("person")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[VERTEX])",
                node.explain().trim());
    }

    // g.V().has('age', 10).hasLabel('software'), property 'age' not exist in label='software'
    @Test
    public void label_equals_5_test() {
        try {
            GraphBuilder builder = Utils.mockGraphBuilder();
            RelNode node =
                    builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                            .filter(
                                    builder.call(
                                            GraphStdOperatorTable.EQUALS,
                                            builder.variable(null, "age"),
                                            builder.literal(10)))
                            .filter(
                                    builder.call(
                                            GraphStdOperatorTable.EQUALS,
                                            builder.variable(null, GraphProperty.LABEL_KEY),
                                            builder.literal("software")))
                            .build();
        } catch (FrontendException e) {
            Assert.assertTrue(
                    e.getMessage()
                            .contains(
                                    "{property=age} not found; expected properties are: [id, name,"
                                            + " lang, creationDate]"));
            return;
        }
        Assert.fail();
    }

    // ~label within ['person']
    @Test
    public void label_within_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RexBuilder rexBuilder = builder.getRexBuilder();
        RelNode node =
                builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                        .filter(
                                rexBuilder.makeIn(
                                        builder.variable(null, GraphProperty.LABEL_KEY),
                                        ImmutableList.of(builder.literal("person"))))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " opt=[VERTEX])",
                node.explain().trim());
    }

    // g.V().hasId(1, 2, 3)
    @Test
    public void unique_key_within_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RexBuilder rexBuilder = builder.getRexBuilder();
        RelNode node =
                builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                        .filter(
                                rexBuilder.makeIn(
                                        builder.variable(null, GraphProperty.ID_KEY),
                                        ImmutableList.of(
                                                builder.literal(1),
                                                builder.literal(2),
                                                builder.literal(3))))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                        + " alias=[_], opt=[VERTEX], uniqueKeyFilters=[SEARCH(_.~id,"
                        + " Sarg[1, 2, 3])])",
                node.explain().trim());
    }
}
