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
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class MatchTest {
    // g.V().optional(match(as("x").hasLabel("person").ouE("knows").as("y")))
    @Test
    public void match_1_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode expand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "y"))
                        .build();
        RelNode match = builder.match(expand, GraphOpt.Match.OPTIONAL).build();
        Assert.assertEquals(
                "GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[OPTIONAL])",
                match.explain().trim());
    }

    // g.V().match(as("x").hasLabel("person").ouE("knows").as("y"),
    // as("x").hasLabel("person").outE("knows").as("z"))
    @Test
    public void match_2_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode expand1 =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "y"))
                        .build();
        RelNode expand2 =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "z"))
                        .build();
        RelNode match = builder.match(expand1, ImmutableList.of(expand2)).build();
        Assert.assertEquals(
                "GraphLogicalMultiMatch(input=[null],"
                        + " sentences=[{s0=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[y], opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])\n"
                        + "], s1=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[z], opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])\n"
                        + "]}])",
                match.explain().trim());
    }
}
