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

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleWrapper;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiLogicalPlan;
import com.alibaba.graphscope.common.ir.runtime.ffi.RelToFfiConverter;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.jna.type.FfiData;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.collect.ImmutableList;
import com.sun.jna.Pointer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FfiLogicalPlanTest {
    // Match (x:person)-[:knows*1..3]->(:person {age: 10})
    @Test
    public void logical_plan_test() throws Exception {
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
                        .resultOpt(GraphOpt.PathExpandResult.AllV)
                        .build();
        RelNode node =
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
                        .build();
        RelNode aggregate =
                builder.match(node, GraphOpt.Match.INNER)
                        .aggregate(builder.groupKey(), builder.count(builder.variable("x")))
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[x],"
                    + " aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[=(DEFAULT.age, 10)]], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[DEFAULT], opt=[END])\n"
                    + "], offset=[1], fetch=[3], path_opt=[SIMPLE], result_opt=[AllV],"
                    + " alias=[DEFAULT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                aggregate.explain().trim());
        try (LogicalPlan<Pointer, FfiData.ByValue> ffiPlan =
                new LogicalPlanConverter(
                                new GraphRelShuttleWrapper(new RelToFfiConverter(true)),
                                new FfiLogicalPlan(
                                        builder.getCluster(), Utils.schemaMeta, getMockPlanHints()))
                        .go(aggregate)) {
            Assert.assertEquals(
                    FileUtils.readJsonFromResource("ffi_logical_plan.json"), ffiPlan.explain());
        }
    }

    private List<RelHint> getMockPlanHints() {
        return ImmutableList.of(
                RelHint.builder("plan")
                        .hintOption("servers", "1")
                        .hintOption("workers", "1")
                        .build());
    }
}
