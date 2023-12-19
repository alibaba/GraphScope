package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.rules.DegreeFusionRule;
import com.alibaba.graphscope.common.ir.planner.rules.ExpandGetVFusionRule;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.GetVConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.PathExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GraphRelToProtoTest {
    @Test
    public void scan_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode scan =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x],"
                        + " opt=[VERTEX])",
                scan.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(scan))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/scan_test.json"), plan.explain().trim());
        }
    }

    @Test
    public void edge_expand_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode expand =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[DEFAULT],"
                        + " opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                expand.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(expand))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void get_v_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode getV =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
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
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[DEFAULT],"
                        + " opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[DEFAULT], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                getV.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(getV))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/getv_test.json"), plan.explain().trim());
        }
    }

    @Test
    public void path_expand_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(builder);
        GetVConfig getVConfig =
                new GetVConfig(GraphOpt.GetV.END, new LabelConfig(false).addLabel("person"));
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
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .pathExpand(pxdConfig)
                        .build();
        Assert.assertEquals(
                "GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}], alias=[DEFAULT], opt=[OUT])\n"
                        + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[END])\n"
                        + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                        + " alias=[DEFAULT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                aggregate.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(aggregate))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/path_expand_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void project_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode project =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .project(
                                ImmutableList.of(builder.variable("x", "name")),
                                ImmutableList.of("name"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(name=[x.name], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                project.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(project))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/project_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void filter_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode filter =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable("x", "name"),
                                        builder.literal("marko")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x],"
                        + " fusedFilter=[[=(DEFAULT.name, _UTF-8'marko')]], opt=[VERTEX])",
                filter.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(filter))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/filter_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void aggregate_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode aggregate =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .aggregate(
                                builder.groupKey(builder.variable("x", "name")),
                                builder.count(builder.variable("x")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[x.name], aliases=[name]}],"
                        + " values=[[{operands=[x], aggFunction=COUNT, alias='$f1',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                aggregate.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(aggregate))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/aggregate_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void dedup_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        Iterable<AggCall> emptyIterable = Collections::emptyIterator;
        RelNode dedup =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .aggregate(builder.groupKey(builder.variable("x", "name")), emptyIterable)
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[x.name], aliases=[name]}],"
                        + " values=[[]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                dedup.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(dedup))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/dedup_test.json"), plan.explain().trim());
        }
    }

    @Test
    public void join_test_1() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode source1 =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .build();
        RelNode source2 =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .build();
        RexNode condition = builder.getJoinCondition(source1, source2);
        builder.push(source1);
        builder.push(source2);
        RelNode join = builder.join(JoinRelType.INNER, condition).build();
        Assert.assertEquals(
                "LogicalJoin(condition=[=(x, x)], joinType=[inner])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                join.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(join))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/join_test_1.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void join_test_2() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode expand1 =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "b"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "x",
                                        "b"))
                        .build();
        RelNode expand2 =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("software"),
                                        "c"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.IN,
                                        new LabelConfig(false).addLabel("created"),
                                        "d"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.START,
                                        new LabelConfig(false).addLabel("person"),
                                        "x",
                                        "d"))
                        .build();
        RexNode condition = builder.getJoinCondition(expand1, expand2);
        builder.push(expand1);
        builder.push(expand2);
        RelNode join = builder.join(JoinRelType.INNER, condition).build();
        Assert.assertEquals(
                "LogicalJoin(condition=[=(x, x)], joinType=[inner])\n"
                        + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], startAlias=[b], opt=[END])\n"
                        + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[b], opt=[OUT])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])\n"
                        + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], startAlias=[d], opt=[START])\n"
                        + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[created]}],"
                        + " alias=[d], opt=[IN])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[software]}],"
                        + " alias=[c], opt=[VERTEX])",
                join.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(join))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/join_test_2.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void sort_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
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
                        + " alias=[DEFAULT], opt=[VERTEX])",
                sort.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(sort))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/sort_test.json"), plan.explain().trim());
        }
    }

    @Test
    public void limit_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode limit =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person")))
                        .limit(0, 10)
                        .build();
        Assert.assertEquals(
                "GraphLogicalSort(fetch=[10])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                limit.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(limit))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/limit_test.json"), plan.explain().trim());
        }
    }

    // g.V().hasLabel("person").out("knows").count()
    @Test
    public void expand_degree_test() throws Exception {
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
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_degree_test.json"),
                    plan.explain().trim());
        }
    }

    // g.V().hasLabel("person").outE("knows").inV()
    @Test
    public void expand_vertex_test() throws Exception {
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
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_vertex_test.json"),
                    plan.explain().trim());
        }
    }

    private Configs getMockGraphConfig() {
        return new Configs(ImmutableMap.of("servers", "1", "workers", "1"));
    }
}
