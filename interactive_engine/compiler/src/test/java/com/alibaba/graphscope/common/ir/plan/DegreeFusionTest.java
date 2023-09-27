package com.alibaba.graphscope.common.ir.plan;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.rules.DegreeFusionRule;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.junit.Assert;
import org.junit.Test;


public class DegreeFusionTest {
    // g.V().out().count()
    @Test
    public void degree_fusion_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence =
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
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "z"))
                        .aggregate(
                                builder.groupKey(),
                                // builder.count(false, "cnt", ImmutableList.of())
                                builder.count(builder.variable("z"))
                        )
                        .build();
        // before
        System.out.println(sentence.explain().trim());
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], " +
                "values=[[{operands=[z], aggFunction=COUNT, alias='$f0', distinct=false}]])\n" +
                "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[z], opt=[END])\n" +
                "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[y], opt=[OUT])\n" +
                "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x], opt=[VERTEX])",
                sentence.explain().trim());
        RelOptPlanner planner = Utils.mockPlanner(DegreeFusionRule.Config.DEFAULT);
        planner.setRoot(sentence);
        RelNode after = planner.findBestExp();
        System.out.println(after.explain().trim());
    }
}