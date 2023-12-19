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

package com.alibaba.graphscope.common.ir.plan;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.rules.ExpandGetVFusionRule;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class ExpandGetVFusionTest {
    // g.V().hasLabel("person").outE("knows").inV(), can be fused into GraphPhysicalExpand
    @Test
    public void expand_getv_fusion_0_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[DEFAULT],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").as("a").inV(), can not be fused
    @Test
    public void expand_getv_fusion_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "a"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[a], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[a], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").inV().as("a"), can be fused into GraphPhysicalExpand
    @Test
    public void expand_getv_fusion_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[a],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                after.explain().trim());
    }

    // TODO(FIXME).
    // g.V().hasLabel("person").outE("knows").inV().has("age",10), can be fused into
    // GraphPhysicalExpand + GraphPhysicalGetV
    @Test
    public void expand_getv_fusion_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, "age"),
                                        builder.literal(10)))
                        .build();

        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[a],"
                        + " fusedFilter=[[=(DEFAULT.age, 10)]], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[a],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                after.explain().trim());
    }
}
