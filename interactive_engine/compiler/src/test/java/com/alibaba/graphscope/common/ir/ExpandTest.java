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

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class ExpandTest {
    // g.V().hasLabel("person").outE("knows")
    @Test
    public void expand_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode expand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                        + " opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                expand.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").as("x")
    @Test
    public void expand_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode expand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "x"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
                        + " opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                expand.explain().trim());
    }

    // path expand: g.V().hasLabel("person").out('1..3', "knows").has("age",
    // eq(10)).with('PATH_OPT',
    // SIMPLE).with('RESULT_OPT', ALL_V)
    @Test
    public void expand_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(builder);
        PathExpandConfig pxdConfig =
                pxdBuilder
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person")))
                        .filter(
                                pxdBuilder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        pxdBuilder.variable(null, "age"),
                                        pxdBuilder.literal(10)))
                        .range(1, 3)
                        .pathOpt(GraphOpt.PathExpandPath.SIMPLE)
                        .resultOpt(GraphOpt.PathExpandResult.ALL_V)
                        .buildConfig();
        RelNode pathExpand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .pathExpand(pxdConfig)
                        .build();
        Assert.assertEquals(
                "GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[_], opt=[OUT])\n"
                        + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], fusedFilter=[[=(_.age, 10)]], opt=[END])\n"
                        + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[_])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                pathExpand.explain().trim());
    }

    // g.V().hasLabel("person").as("a").outE("knows").select("a").outE("knows").as("b")
    @Test
    public void expand_4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode expand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "b",
                                        "a"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                        + " startAlias=[a], opt=[OUT])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                expand.explain().trim());
    }

    // g.V().hasLabel("person").as("a").outE("knows").select("a").out("1..3", "knows")
    @Test
    public void expand_5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(builder);
        PathExpandConfig pxdConfig =
                pxdBuilder
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person")))
                        .range(1, 3)
                        .startAlias("a")
                        .buildConfig();
        RelNode expand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .pathExpand(pxdConfig)
                        .build();
        Assert.assertEquals(
                "GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[_], opt=[OUT])\n"
                        + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[END])\n"
                        + "], offset=[1], fetch=[3], path_opt=[ARBITRARY], result_opt=[END_V],"
                        + " alias=[_], start_alias=[a])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                expand.explain().trim());
    }
}
