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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;

public class FfiLogicalPlanTest {
    // Match (x:person)-[:knows*1..3]->(:person {age: 10})
    // Return count(*)
    @Test
    public void logical_plan_1_test() throws Exception {
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
                        .getV(getVConfig)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        pxdBuilder.variable(null, "age"),
                                        pxdBuilder.literal(10)))
                        .aggregate(builder.groupKey(), builder.count(builder.variable("x")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[x],"
                    + " aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[=(DEFAULT.age, 10)]], opt=[END])\n"
                    + "    GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[DEFAULT], opt=[END])\n"
                    + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[ALL_V],"
                    + " alias=[DEFAULT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])",
                aggregate.explain().trim());
        try (PhysicalBuilder<byte[]> ffiBuilder =
                new FfiPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(aggregate))) {
            ffiBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("ffi_logical_plan_1.json"),
                    ffiBuilder.explain());
        }
    }

    // Match (x:person) where x.age = $age
    @Test
    public void logical_plan_2_test() throws Exception {
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
                                        builder.variable("x", "age"),
                                        ((GraphRexBuilder) builder.getRexBuilder())
                                                .makeGraphDynamicParam("age", 0)))
                        .build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}], alias=[x],"
                        + " fusedFilter=[[=(DEFAULT.age, ?0)]], opt=[VERTEX])",
                filter.explain().trim());
        try (PhysicalBuilder<byte[]> ffiBuilder =
                new FfiPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(filter))) {
            ffiBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("ffi_logical_plan_2.json"),
                    ffiBuilder.explain());
        }
    }

    // Match (n) Return distinct n;
    @Test
    public void logical_plan_3_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode project =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "n"))
                        .aggregate(builder.groupKey(builder.variable("n")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[n], aliases=[n]}], values=[[]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[n], opt=[VERTEX])",
                project.explain().trim());
        try (PhysicalBuilder<byte[]> ffiBuilder =
                new FfiPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(project))) {
            ffiBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("ffi_logical_plan_3.json"),
                    ffiBuilder.explain());
        }
    }

    @Test
    public void logical_plan_4_test() throws Exception {
        LogicalPlan logicalPlan =
                com.alibaba.graphscope.cypher.antlr4.Utils.evalLogicalPlan(
                        "Call ldbc_ic2(10l, 20120112l)");
        try (PhysicalBuilder<byte[]> ffiBuilder = new ProcedurePhysicalBuilder(logicalPlan)) {
            ffiBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("call_procedure.json"), ffiBuilder.explain());
        }
    }

    // Match (a:person) Return case when a.name = 'marko' then 1 else 3 end as case
    @Test
    public void logical_plan_5_test() throws Exception {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"));
        RexNode caseExpr =
                builder.call(
                        GraphStdOperatorTable.CASE,
                        builder.call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable("a", "name"),
                                builder.literal("marko")),
                        builder.literal(1),
                        builder.literal(3));
        RelNode project =
                builder.project(ImmutableList.of(caseExpr), ImmutableList.of("case")).build();
        Assert.assertEquals(
                "GraphLogicalProject(case=[CASE(=(a.name, 'marko'), 1, 3)], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
        try (PhysicalBuilder<byte[]> ffiBuilder =
                new FfiPhysicalBuilder(
                        getMockGraphConfig(), Utils.schemaMeta, new LogicalPlan(project))) {
            ffiBuilder.build();
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("case_when.json"), ffiBuilder.explain());
        }
    }

    private Configs getMockGraphConfig() {
        return new Configs(ImmutableMap.of("servers", "1", "workers", "1"));
    }
}
