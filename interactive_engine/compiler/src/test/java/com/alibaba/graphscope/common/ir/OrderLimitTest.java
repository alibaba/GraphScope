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
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class OrderLimitTest {
    // g.V().order().by("name")
    @Test
    public void order_1_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode sort =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .sort(builder.variable(null, "name"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[DEFAULT.name], dir0=[ASC])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[~DEFAULT], opt=[VERTEX])",
                sort.explain().trim());
    }

    // g.V().order().by("name", desc)
    @Test
    public void order_2_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode sort =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .sort(builder.desc(builder.variable(null, "name")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[DEFAULT.name], dir0=[DESC])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[~DEFAULT], opt=[VERTEX])",
                sort.explain().trim());
    }

    // g.V().order().by("name").limit(1, 2) -> topK
    @Test
    public void order_3_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode sort =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .sort(builder.desc(builder.variable(null, "name")))
                        .limit(1, 2)
                        .build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[DEFAULT.name], dir0=[DESC], offset=[1], fetch=[2])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[~DEFAULT], opt=[VERTEX])",
                sort.explain().trim());
    }

    // g.V().limit(1, 2)
    @Test
    public void order_4_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode limit =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .limit(1, 2)
                        .build();
        Assert.assertEquals(
                "GraphLogicalSort(offset=[1], fetch=[2])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[~DEFAULT], opt=[VERTEX])",
                limit.explain().trim());
    }

    // order by HEAD.age+1 DESC -> project({HEAD.age+1 as '$f0'}, isAppend = true) + order by $f0
    // desc + project(original, isAppend = false)
    @Test
    public void order_5_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode sort =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .sort(
                                builder.desc(
                                        builder.call(
                                                GraphStdOperatorTable.PLUS,
                                                builder.variable(null, "age"),
                                                builder.literal(1))))
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(~DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalSort(sort0=[$f0], dir0=[DESC])\n"
                        + "    GraphLogicalProject($f0=[+(DEFAULT.age, 1)], isAppend=[true])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[~DEFAULT], opt=[VERTEX])",
                sort.explain().trim());
    }
}
