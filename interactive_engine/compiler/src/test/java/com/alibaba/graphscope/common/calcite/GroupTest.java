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

package com.alibaba.graphscope.common.calcite;

import com.alibaba.graphscope.common.calcite.tools.GraphBuilder;
import com.alibaba.graphscope.common.calcite.tools.config.*;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class GroupTest {
    // g.V().hasLabel("person").group().by(values("name").as("a"),
    // values("age").as("b")).by(count().as("c"))
    @Test
    public void group_1_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        ScanOpt.Vertex, new LabelConfig(false).addLabel("person")))
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
                        + " b]}], values=[[{operands=[], aggFunction=COUNT, alias='c'}]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[~DEFAULT], opt=[Vertex])",
                aggregate.explain().trim());
    }
}
