package com.alibaba.graphscope.common.ir.plan;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.junit.Assert;
import org.junit.Test;

public class HepPlannerTest {
    // Match(x:person)-[y:knows]->() Where y.weight = 1.0 -> Match(x:person)-[y:knows {weight: 1.0}]
    @Test
    public void push_filter_1_test() {
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
                        .build();
        RelNode filter =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable("y", "weight"),
                                        builder.literal(1.0)))
                        .build();
        RelOptPlanner planner = Utils.mockPlanner(FilterMatchRule.Config.DEFAULT);
        planner.setRoot(filter);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], fusedFilter=[[=(DEFAULT.weight, 1.0E0)]], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
    }

    // Match(x:person)-[:knows*1..3]->(z:person) Where z.age = 10 ->
    // Match(x:person)-[:knows*1..3]->(z:person {age: 10})
    @Test
    public void push_filter_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(builder);
        GetVConfig getVConfig =
                new GetVConfig(GraphOpt.GetV.END, new LabelConfig(false).addLabel("person"), "z");
        PathExpandConfig pxdConfig =
                pxdBuilder
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .getV(getVConfig)
                        .range(1, 3)
                        .pathOpt(GraphOpt.PathExpandPath.SIMPLE)
                        .resultOpt(GraphOpt.PathExpandResult.ALL_V)
                        .build();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .pathExpand(pxdConfig)
                        .getV(getVConfig)
                        .build();
        RelNode before =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable("z", "age"),
                                        builder.literal(10)))
                        .build();
        RelOptPlanner planner = Utils.mockPlanner(FilterMatchRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[z], fusedFilter=[[=(DEFAULT.age, 10)]], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[DEFAULT], opt=[END])\n"
                    + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                    + " alias=[DEFAULT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
    }

    // Match(x:person)-[y:knows]->() Where x.age = 10 or x.age = 20 -> Match(x:person {x.age = 10 or
    // 20})-[y:knows]->()
    @Test
    public void push_filter_3_test() {
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
                        .build();
        RelNode before =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.OR,
                                        builder.call(
                                                GraphStdOperatorTable.EQUALS,
                                                builder.variable("x", "age"),
                                                builder.literal(10)),
                                        builder.call(
                                                GraphStdOperatorTable.EQUALS,
                                                builder.variable("x", "age"),
                                                builder.literal(20))))
                        .build();
        RelOptPlanner planner = Utils.mockPlanner(FilterMatchRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], fusedFilter=[[OR(=(DEFAULT.age, 10), =(DEFAULT.age, 20))]],"
                    + " opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
    }

    // push filter down to match
    @Test
    public void push_filter_4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence1 =
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
        RelNode sentence2 =
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
        RelNode before =
                builder.match(sentence1, GraphOpt.Match.INNER)
                        .match(sentence2, GraphOpt.Match.INNER)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.AND,
                                        builder.call(
                                                GraphStdOperatorTable.EQUALS,
                                                builder.variable("y", "weight"),
                                                builder.literal(10)),
                                        builder.call(
                                                GraphStdOperatorTable.EQUALS,
                                                builder.variable("z", "weight"),
                                                builder.literal(20))))
                        .build();
        Assert.assertEquals(
                "LogicalFilter(condition=[AND(=(y.weight, 10), =(z.weight, 20))])\n"
                    + "  LogicalJoin(condition=[=(x, x)], joinType=[inner])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[z], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                before.explain().trim());

        // check plan after applying FILTER_INTO_JOIN rule from calcite
        RelOptPlanner planner = Utils.mockPlanner(CoreRules.FILTER_INTO_JOIN.config);
        planner.setRoot(before);
        RelNode after1 = planner.findBestExp();
        Assert.assertEquals(
                "LogicalJoin(condition=[=(x, x)], joinType=[inner])\n"
                    + "  LogicalFilter(condition=[=(y.weight, 10)])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])\n"
                    + "  LogicalFilter(condition=[=(z.weight, 20)])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[z], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after1.explain().trim());

        // check plan after applying FILTER_INTO_JOIN and FILTER_MATCH rules
        planner =
                Utils.mockPlanner(
                        CoreRules.FILTER_INTO_JOIN.config, FilterMatchRule.Config.DEFAULT);
        planner.setRoot(before);
        RelNode after2 = planner.findBestExp();
        Assert.assertEquals(
                "LogicalJoin(condition=[=(x, x)], joinType=[inner])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], fusedFilter=[[=(DEFAULT.weight, 10)]], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[z], fusedFilter=[[=(DEFAULT.weight, 20)]], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after2.explain().trim());
    }

    @Test
    public void push_filter_5_test() {
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[b]-(c:person) Where labels(a)='person' and a.name ="
                                        + " 'marko' Return a")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[=(a.name, _UTF-8'marko')])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                node.explain().trim());
        RelOptPlanner planner = Utils.mockPlanner(FilterMatchRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], fusedFilter=[[=(DEFAULT.name, _UTF-8'marko')]], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
    }
}
