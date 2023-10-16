package com.alibaba.graphscope.common.ir.plan;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.rules.DegreeFusionRule;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;

public class DegreeFusionTest {

    // g.V().hasLabel("person").out().hasLabel("knows").count()
    @Test
    public void degree_fusion_0_test() {
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
                                builder.groupKey(), // global key, i.e. g.V().count()
                                // builder.countStar("cnt")
                                builder.count(false, "cnt", ImmutableList.of()))
                        .build();
        // before
        System.out.println(sentence.explain().trim());

        RelOptPlanner planner = Utils.mockPlanner(DegreeFusionRule.Config.DEFAULT);
        planner.setRoot(sentence);
        RelNode after = planner.findBestExp();
        System.out.println(after.explain().trim());
    }

    // source("person").as("x").expand("knows").as("y").getV("persons").as("z").count()
    //    @Test
    //    public void degree_fusion_1_test() {
    //        GraphBuilder builder = Utils.mockGraphBuilder();
    //        RelNode sentence =
    //                builder.source(
    //                                new SourceConfig(
    //                                        GraphOpt.Source.VERTEX,
    //                                        new LabelConfig(false).addLabel("person"),
    //                                        "x"))
    //                        .expand(
    //                                new ExpandConfig(
    //                                        GraphOpt.Expand.OUT,
    //                                        new LabelConfig(false).addLabel("knows"),
    //                                        "y"))
    //                        .getV(
    //                                new GetVConfig(
    //                                        GraphOpt.GetV.END,
    //                                        new LabelConfig(false).addLabel("person"),
    //                                        "z"))
    //                        .aggregate(
    //                                builder.groupKey(), // global key, i.e. g.V().count()
    //                                // builder.count(false, "cnt", ImmutableList.of())
    //                                //
    //                                // builder.count(builder.variable("z")))
    //                                builder.countStar("cnt"))
    //                        .build();
    //        // before
    //        System.out.println(sentence.explain().trim());
    //        RelOptPlanner planner = Utils.mockPlanner(DegreeFusionRule.Config.DEFAULT);
    //        planner.setRoot(sentence);
    //        RelNode after = planner.findBestExp();
    //        System.out.println(after.explain().trim());
    //    }

    // don't match this rule
    //    @Test
    //    public void degree_fusion_2_test() {
    //        GraphBuilder builder = Utils.mockGraphBuilder();
    //        RelNode sentence =
    //                builder.source(
    //                                new SourceConfig(
    //                                        GraphOpt.Source.VERTEX,
    //                                        new LabelConfig(false).addLabel("person"),
    //                                        "x"))
    //                        .expand(
    //                                new ExpandConfig(
    //                                        GraphOpt.Expand.OUT,
    //                                        new LabelConfig(false).addLabel("knows"),
    //                                        "y"))
    //                        .getV(
    //                                new GetVConfig(
    //                                        GraphOpt.GetV.END,
    //                                        new LabelConfig(false).addLabel("person"),
    //                                        "z"))
    //                        .aggregate(
    //                                builder.groupKey(
    //                                        ImmutableList.of(builder.variable(null, "name")),
    //                                        ImmutableList.of("a")),
    //                                builder.collect(false, "c", ImmutableList.of()))
    //                        .build();
    //
    //        RelOptPlanner planner = Utils.mockPlanner(DegreeFusionRule.Config.DEFAULT);
    //        planner.setRoot(sentence);
    //        RelNode after = planner.findBestExp();
    //        Assert.assertEquals(
    //                "GraphLogicalAggregate(keys=[{variables=[DEFAULT.name], aliases=[a]}],"
    //                    + " values=[[{operands=[DEFAULT], aggFunction=COLLECT, alias='c',"
    //                    + " distinct=false}]])\n"
    //                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],
    // alias=[z],"
    //                    + " opt=[END])\n"
    //                    + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
    //                    + " alias=[y], opt=[OUT])\n"
    //                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
    //                    + " alias=[x], opt=[VERTEX])",
    //                after.explain().trim());
    //    }
}
