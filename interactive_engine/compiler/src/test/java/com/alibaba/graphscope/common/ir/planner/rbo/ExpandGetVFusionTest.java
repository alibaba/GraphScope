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

package com.alibaba.graphscope.common.ir.planner.rbo;

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
                        + " alias=[_], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
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
                        + " alias=[_], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[a], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[a], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
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
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[a],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").has("weight",0.5).inV(), can be fused into
    // GraphPhysicalExpand
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
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, "weight"),
                                        builder.literal(0.5)))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], fusedFilter=[[=(_.weight, 5E-1)]], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                        + " fusedFilter=[[=(_.weight, 5E-1)]], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").inV().as("a").has("age",10), can be fused into
    // GraphPhysicalExpand + GraphPhysicalGetV
    @Test
    public void expand_getv_fusion_4_test() {
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
                        + " fusedFilter=[[=(_.age, 10)]], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[a],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[END], physicalOpt=[ITSELF])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").has("weight",0.5).inV().as("a").has("age",10), can be
    // fused into
    // GraphPhysicalExpand + GraphPhysicalGetV
    @Test
    public void expand_getv_fusion_5_test() {
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
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, "weight"),
                                        builder.literal(0.5)))
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
                        + " fusedFilter=[[=(_.age, 10)]], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], fusedFilter=[[=(_.weight, 5E-1)]], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[a],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[END], physicalOpt=[ITSELF])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], fusedFilter=[[=(_.weight, 5E-1)]], opt=[OUT],"
                        + " physicalOpt=[VERTEX])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").outV(), can not be fused
    @Test
    public void expand_getv_fusion_6_test() {
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
                                        GraphOpt.GetV.START,
                                        new LabelConfig(false).addLabel("person")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[START])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[START])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("likes").inV().hasLabel("comment"), can not be fused into a
    // single GraphPhysicalExpand since we actually have person-likes-comment and person-likes-post.
    @Test
    public void expand_getv_fusion_7_test() {
        GraphBuilder builder = Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("PERSON")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("LIKES")))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("COMMENT")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                        + " alias=[_], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}], alias=[_],"
                        + " opt=[END], physicalOpt=[ITSELF])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                        + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("likes").inV(), can be fused into a single GraphPhysicalExpand
    // even without type infer for GetV
    @Test
    public void expand_getv_fusion_8_test() {
        GraphBuilder builder = Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("PERSON")))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("LIKES")))
                        .getV(new GetVConfig(GraphOpt.GetV.END, new LabelConfig(true)))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=true, tables=[PERSON, POST, TAG,"
                    + " ORGANISATION, PLACE, TAGCLASS, COMMENT, FORUM]}], alias=[_], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}], alias=[_],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[_], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}], alias=[_],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // path expand: g.V().hasLabel("person").out('1..3', "knows").with('PATH_OPT',
    // SIMPLE).with('RESULT_OPT', ALL_V)
    @Test
    public void path_expand_getv_fusion_0_test() {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")));
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
                        .pathOpt(GraphOpt.PathExpandPath.SIMPLE)
                        .resultOpt(GraphOpt.PathExpandResult.ALL_V)
                        .buildConfig();
        RelNode before =
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
                        + " alias=[_], opt=[END])\n"
                        + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[_])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());

        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.PathBaseExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n],"
                        + " offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[_])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // path expand: g.V().hasLabel("person").out('1..3', "knows").has("age",
    // eq(10)).with('PATH_OPT', SIMPLE).with('RESULT_OPT', ALL_V)
    @Test
    public void path_expand_getv_fusion_1_test() {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")));
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
        RelNode before =
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
                before.explain().trim());

        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.PathBaseExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalPathExpand(fused=[GraphPhysicalGetV(tableConfig=[{isAll=false,"
                        + " tables=[person]}], alias=[_], fusedFilter=[[=(_.age, 10)]],"
                        + " opt=[END], physicalOpt=[ITSELF])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[_])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    // path expand with edge filters
    @Test
    public void path_expand_getv_fusion_2_test() {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")));
        PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(builder);
        PathExpandConfig pxdConfig =
                pxdBuilder
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .filter(
                                pxdBuilder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        pxdBuilder.variable(null, "weight"),
                                        pxdBuilder.literal(0.5)))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person")))
                        .range(1, 3)
                        .pathOpt(GraphOpt.PathExpandPath.SIMPLE)
                        .resultOpt(GraphOpt.PathExpandResult.ALL_V)
                        .buildConfig();
        RelNode before =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .pathExpand(pxdConfig)
                        .build();
        Assert.assertEquals(
                "GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[_], fusedFilter=[[=(_.weight, 5E-1)]],"
                        + " opt=[OUT])\n"
                        + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[END])\n"
                        + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[_])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                before.explain().trim());

        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.PathBaseExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[_], fusedFilter=[[=(_.weight, 5E-1)]],"
                        + " opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[_])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }
}
