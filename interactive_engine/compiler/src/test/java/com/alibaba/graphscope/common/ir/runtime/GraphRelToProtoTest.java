/*
 * Copyright 2024 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
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
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(scan), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/scan_test.json"), plan.explain().trim());
        }
    }

    @Test
    public void scan_edge_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode scan =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.EDGE,
                                        new LabelConfig(false).addLabel("knows"),
                                        "x"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
                        + " opt=[EDGE])",
                scan.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(scan), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/scan_edge_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void scan_filter_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode scan =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, "age"),
                                        builder.literal(10)))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[VERTEX])",
                scan.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(scan), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/scan_filter_test.json"),
                    plan.explain().trim());
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
                "GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                        + " opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                expand.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(expand), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_test.json"),
                    plan.explain().trim());
        }

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(expand))) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_edge_expand_test.json"),
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
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                getV.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(getV), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/getv_test.json"), plan.explain().trim());
        }

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(getV),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_getv_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void get_v_with_filter_test() throws Exception {
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
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable(null, "age"),
                                        builder.literal(10)))
                        .build();
        Assert.assertEquals(
                "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[_],"
                        + " fusedFilter=[[=(_.age, 10)]], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                getV.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(getV), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/getv_with_filter_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void path_expand_test() throws Exception {
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
        RelNode pxd =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
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
                        + " alias=[x], opt=[VERTEX])",
                pxd.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(pxd), true)) {
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
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(project), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/project_test.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(project),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_project_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void project_02_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode project =
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
                        .project(
                                ImmutableList.of(
                                        builder.variable("x", "name"),
                                        builder.variable("y", "weight")),
                                ImmutableList.of("name"))
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(name=[x.name], weight=[y.weight],"
                        + " isAppend=[false])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[y], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                project.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(project), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/project_test_2.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(project),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_project_test_2.json"),
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
                        + " fusedFilter=[[=(_.name, _UTF-8'marko')]], opt=[VERTEX])",
                filter.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(filter), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/filter_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void filter_test_2() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode filter =
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
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.GREATER_THAN,
                                        builder.variable("x", "age"),
                                        builder.variable("y", "weight")))
                        .build();
        Assert.assertEquals(
                "LogicalFilter(condition=[>(x.age, y.weight)])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[y],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])",
                filter.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(filter), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/filter_test_2.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(filter),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_filter_test_2.json"),
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
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(aggregate), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/aggregate_test.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(aggregate),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_aggregate_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void aggregate_test_2() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode aggregate =
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
                        .aggregate(
                                builder.groupKey(
                                        builder.variable("x", "name"),
                                        builder.variable("y", "weight")),
                                builder.collect(
                                        ImmutableList.of(
                                                builder.variable("x", "name"),
                                                builder.variable("y", "weight"))))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[x.name, y.weight], aliases=[name,"
                    + " weight]}], values=[[{operands=[x.name, y.weight], aggFunction=COLLECT,"
                    + " alias='$f2', distinct=false}]])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[y],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])",
                aggregate.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(aggregate), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/aggregate_test_2.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void dedup_test_1() throws Exception {
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
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(dedup), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/dedup_test_1.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(dedup),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_dedup_test_1.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void dedup_test_2() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode dedup =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .dedupBy(ImmutableList.of(builder.variable("x", "name")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalDedupBy(dedupByKeys=[[x.name]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])",
                dedup.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(dedup), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/dedup_test_2.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(dedup),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_dedup_test_2.json"),
                    plan.explain().trim());
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
                                        "y"))
                        .build();

        builder.push(source1);
        builder.push(source2);
        RelNode join =
                builder.join(
                                JoinRelType.INNER,
                                builder.equals(
                                        builder.variable("x", "name"),
                                        builder.variable("y", "name")))
                        .build();
        Assert.assertEquals(
                "LogicalJoin(condition=[=(x.name, y.name)], joinType=[inner])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[y], opt=[VERTEX])",
                join.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(join), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/join_test_1.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(join),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_join_test_1.json"),
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
                                        "y",
                                        "d"))
                        .build();
        builder.push(expand1);
        builder.push(expand2);
        RelNode join =
                builder.join(
                                JoinRelType.INNER,
                                builder.equals(
                                        builder.variable("x", "name"),
                                        builder.variable("y", "name")))
                        .build();
        Assert.assertEquals(
                "LogicalJoin(condition=[=(x.name, y.name)], joinType=[inner])\n"
                        + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], startAlias=[b], opt=[END])\n"
                        + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[b], opt=[OUT])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])\n"
                        + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[y], startAlias=[d], opt=[START])\n"
                        + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[created]}],"
                        + " alias=[d], opt=[IN])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[software]}],"
                        + " alias=[c], opt=[VERTEX])",
                join.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(join), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/join_test_2.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(join),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_join_test_2.json"),
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
                "GraphLogicalSort(sort0=[_.name], dir0=[ASC])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                sort.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(sort), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/sort_test.json"), plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(sort),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_sort_test.json"),
                    plan.explain().trim());
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
                        + " alias=[_], opt=[VERTEX])",
                limit.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(limit), true)) {
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
                        + " values=[[{operands=[_], aggFunction=$SUM0, alias='cnt',"
                        + " distinct=false}]])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT], physicalOpt=[DEGREE])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
                after.explain().trim());
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_degree_test.json"),
                    plan.explain().trim());
        }

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource(
                            "proto/partitioned_edge_expand_degree_test.json"),
                    plan.explain().trim());
        }
    }

    // g.V().hasLabel("person").outE("knows").inV().as("a")
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
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_vertex_test.json"),
                    plan.explain().trim());
        }

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource(
                            "proto/partitioned_edge_expand_vertex_test.json"),
                    plan.explain().trim());
        }
    }

    // g.V().hasLabel("person").outE("knows").inV().as("a").has("age",10), can be fused into
    // GraphPhysicalExpand + GraphPhysicalGetV
    @Test
    public void expand_vertex_filter_test() throws Exception {
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
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/edge_expand_vertex_filter_test.json"),
                    plan.explain().trim());
        }

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedGraphConfig(),
                        Utils.schemaMeta,
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource(
                            "proto/partitioned_edge_expand_vertex_filter_test.json"),
                    plan.explain().trim());
        }
    }

    // g.V().hasLabel("person").outE("knows").has("weight", 0.5).inV().has("age", 10)
    @Test
    public void expand_vertex_with_filters_test() throws Exception {
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
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource(
                            "proto/edge_expand_vertex_with_filters_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void path_expand_fused_test() throws Exception {
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
        RelNode pxd =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
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
                        + " alias=[x], opt=[VERTEX])",
                pxd.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.PathBaseExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(pxd);
        RelNode after = planner.findBestExp();
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(after), true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/path_fused_expand_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void intersect_test() throws Exception {
        GraphRelOptimizer optimizer = getMockCBO();
        IrMeta irMeta = getMockCBOMeta(optimizer);
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (message:COMMENT|POST)-[:HASCREATOR]->(person:PERSON), \n"
                                        + "      (message:COMMENT|POST)-[:HASTAG]->(tag:TAG), \n"
                                        + "      (person:PERSON)-[:HASINTEREST]->(tag:TAG)\n"
                                        + "Return count(person);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(tag, tag)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[tag], startAlias=[message],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-697155798]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[tag], startAlias=[person], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-697155798]])\n"
                    + "common#-697155798:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[message], startAlias=[person], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockCBOConfig(),
                        getMockCBOMeta(optimizer),
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/intersect_test.json"),
                    plan.explain().trim());
        }

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedCBOConfig(),
                        getMockCBOMeta(optimizer),
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_intersect_test.json"),
                    plan.explain().trim());
        }
    }

    @Test
    public void intersect_test_02() throws Exception {
        GraphRelOptimizer optimizer = getMockCBO();
        IrMeta irMeta = getMockCBOMeta(optimizer);
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (message:COMMENT|POST)-[e1:HASCREATOR]->(person:PERSON), \n"
                                        + "      (message:COMMENT|POST)-[e2:HASTAG]->(tag:TAG), \n"
                                        + "      (person:PERSON)-[e3:HASINTEREST]->(tag:TAG)\n"
                                        + "Return count(person);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(tag, tag)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[TAG]}], alias=[tag],"
                    + " opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[e2], startAlias=[message],"
                    + " opt=[OUT])\n"
                    + "        CommonTableScan(table=[[common#-676410541]])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[TAG]}], alias=[tag],"
                    + " opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[e3], startAlias=[person], opt=[OUT])\n"
                    + "        CommonTableScan(table=[[common#-676410541]])\n"
                    + "common#-676410541:\n"
                    + "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], opt=[START])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[e1], startAlias=[person], opt=[IN])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());

        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockCBOConfig(),
                        getMockCBOMeta(optimizer),
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/intersect_test_2.json"),
                    plan.explain().trim());
        }
        try (PhysicalBuilder protoBuilder =
                new GraphRelProtoPhysicalBuilder(
                        getMockPartitionedCBOConfig(),
                        getMockCBOMeta(optimizer),
                        new LogicalPlan(after),
                        true)) {
            PhysicalPlan plan = protoBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("proto/partitioned_intersect_test_2.json"),
                    plan.explain().trim());
        }
    }

    private Configs getMockCBOConfig() {
        return new Configs(
                ImmutableMap.of(
                        "graph.planner.is.on",
                        "true",
                        "graph.planner.opt",
                        "CBO",
                        "graph.planner.rules",
                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule"));
    }

    private Configs getMockPartitionedCBOConfig() {
        return new Configs(
                ImmutableMap.of(
                        "graph.planner.is.on",
                        "true",
                        "graph.planner.opt",
                        "CBO",
                        "graph.planner.rules",
                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule",
                        "pegasus.hosts",
                        "host1,host2"));
    }

    private GraphRelOptimizer getMockCBO() {
        return new GraphRelOptimizer(getMockCBOConfig());
    }

    private IrMeta getMockCBOMeta(GraphRelOptimizer optimizer) {
        return Utils.mockIrMeta(
                "schema/ldbc_schema_exp_hierarchy.json",
                "statistics/ldbc30_hierarchy_statistics.json",
                optimizer.getGlogueHolder());
    }

    private Configs getMockGraphConfig() {
        return new Configs(ImmutableMap.of("pegasus.hosts", "1", "pegasus.worker.num", "1"));
    }

    private Configs getMockPartitionedGraphConfig() {
        return new Configs(
                ImmutableMap.of("pegasus.hosts", "host1,host2", "pegasus.worker.num", "2"));
    }
}
