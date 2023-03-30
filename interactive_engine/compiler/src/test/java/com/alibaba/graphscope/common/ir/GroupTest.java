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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class GroupTest {
    // g.V().hasLabel("person").group().by(values("name").as("a"),
    // values("age").as("b")).by(count().as("c"))
    @Test
    public void group_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .aggregate(
                                builder.groupKey(
                                        ImmutableList.of(
                                                builder.variable(null, "name"),
                                                builder.variable(null, "age")),
                                        ImmutableList.of("a", "b")),
                                builder.count(false, "c", ImmutableList.of()))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[DEFAULT.name, DEFAULT.age], aliases=[a,"
                        + " b]}], values=[[{operands=[DEFAULT], aggFunction=COUNT, alias='c',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                aggregate.explain().trim());
    }

    // group().by("name").by(sum("@.age+1").as('X')) -> project({HEAD.age+1 as '$f0'}, isAppend =
    // true)
    // + aggregate(keys={HEAD.name}, calls=[sum($f0) as 'x'])
    @Test
    public void group_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .aggregate(
                                builder.groupKey(builder.variable(null, "name")),
                                builder.sum(
                                        false,
                                        "x",
                                        builder.call(
                                                GraphStdOperatorTable.PLUS,
                                                builder.variable(null, "age"),
                                                builder.literal(1))))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[DEFAULT.name], aliases=[name]}],"
                    + " values=[[{operands=[$f0], aggFunction=SUM, alias='x', distinct=false}]])\n"
                    + "  GraphLogicalProject($f0=[+(DEFAULT.age, 1)], isAppend=[true])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX])",
                aggregate.explain().trim());
    }

    // g.V().hasLabel("person").group().by(values("name").as("a"),
    // values("age").as("b")).by(fold().as("c"))
    @Test
    public void group_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .aggregate(
                                builder.groupKey(
                                        ImmutableList.of(
                                                builder.variable(null, "name"),
                                                builder.variable(null, "age")),
                                        ImmutableList.of("a", "b")),
                                builder.collect(false, "c", ImmutableList.of()))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[DEFAULT.name, DEFAULT.age], aliases=[a,"
                        + " b]}], values=[[{operands=[DEFAULT], aggFunction=COLLECT, alias='c',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                aggregate.explain().trim());
    }

    // g.V().hasLabel("person").group().by("@.age+1").by(count().as("c"))
    @Test
    public void group_4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .aggregate(
                                builder.groupKey(
                                        builder.call(
                                                GraphStdOperatorTable.PLUS,
                                                builder.variable(null, "age"),
                                                builder.literal(1))),
                                builder.count(false, "c", ImmutableList.of()))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[$f0], aliases=[$f0]}],"
                        + " values=[[{operands=[DEFAULT], aggFunction=COUNT, alias='c',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalProject($f0=[+(DEFAULT.age, 1)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                aggregate.explain().trim());
    }

    // invalid query: g.group().by("@.age+1")
    @Test
    public void group_5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        try {
            RelNode aggregate =
                    builder.aggregate(
                                    builder.groupKey(
                                            builder.call(
                                                    GraphStdOperatorTable.PLUS,
                                                    builder.variable(null, "age"),
                                                    builder.literal(1))),
                                    builder.count(false, "c", ImmutableList.of()))
                            .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "{alias=DEFAULT} not found; expected aliases are: []", e.getMessage());
            return;
        }
        Assert.fail("alias=DEFAULT does not exist");
    }
}
