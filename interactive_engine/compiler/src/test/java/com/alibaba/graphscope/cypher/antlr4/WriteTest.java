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

package com.alibaba.graphscope.cypher.antlr4;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                        + " ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                com.alibaba.graphscope.common.ir.Utils.mockIrMeta(
                        "schema/movie.json", "statistics/movie_statistics.json", optimizer);
    }

    @Test
    public void add_vertex_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                Utils.eval("CREATE (charlie:Person {name: 'Charlie', id: 211, age: 32})", builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        LogicalPlan plan = new LogicalPlan(after);
        System.out.println(plan.getMode());
    }

    @Test
    public void add_edge_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                Utils.eval(
                                "CREATE (charlie:Person {id: 211})-[:ACTED_IN]->(movie:Movie {name:"
                                        + " 'Wall Street'})",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(after.explain());
    }

    @Test
    public void batch_add_edge_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                Utils.eval(
                                "CREATE (charlie:PERSON {id: [933,"
                                    + " 123]})-[:ISLOCATEDIN]->(place:PLACE {name: 'Wall Street'})",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(after.explain());
    }

    @Test
    public void set_vertex_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                Utils.eval(
                                "MATCH (charlie:Person {id: 211}) Set charlie.age = 33, charlie.id"
                                        + " = 123, charlie.name = 'julie' Return charlie",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(after.explain());
    }
}
