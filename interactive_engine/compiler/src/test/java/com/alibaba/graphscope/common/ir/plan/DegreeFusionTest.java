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
import com.alibaba.graphscope.common.ir.planner.rules.DegreeFusionRule;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class DegreeFusionTest {

    // g.V().hasLabel("person").out("knows").count()
    @Test
    public void degree_fusion_0_test() {
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
                        .aggregate(builder.groupKey(), builder.countStar("cnt"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                        + " values=[[{operands=[DEFAULT], aggFunction=COUNT, alias='cnt',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[END])\n"
                        + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT], opt=[OUT])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(DegreeFusionRule.ExpandGetVDegreeFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                        + " values=[[{operands=[DEFAULT], aggFunction=$SUM0, alias='cnt',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalExpandDegree(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                after.explain().trim());
    }

    // g.V().hasLabel("person").outE("knows").count()
    @Test
    public void degree_fusion_1_test() {
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
                        .aggregate(builder.groupKey(), builder.countStar("cnt"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                        + " values=[[{operands=[DEFAULT], aggFunction=COUNT, alias='cnt',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                before.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(DegreeFusionRule.ExpandDegreeFusionRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                        + " values=[[{operands=[DEFAULT], aggFunction=$SUM0, alias='cnt',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalExpandDegree(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                after.explain().trim());
    }
}
