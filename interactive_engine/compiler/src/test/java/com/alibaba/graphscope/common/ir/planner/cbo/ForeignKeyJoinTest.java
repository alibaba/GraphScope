/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.planner.cbo;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ForeignKeyJoinTest {
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
                                        + " JoinDecompositionRule, ExpandGetVFusionRule",
                                "graph.planner.join.min.pattern.size",
                                "4",
                                "graph.foreign.key",
                                "src/test/resources/schema/ldbc_foreign_key.json"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc30_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void foreign_key_join_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before1 =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p1:PERSON"
                                    + " {id:933})-[:KNOWS]-(p2:PERSON)<-[:HASCREATOR]-(c1:COMMENT)-[:ISLOCATEDIN]->(pl1:PLACE"
                                    + " {name:'India'})\n"
                                    + "  RETURN p2.id, p2.firstName, p2.lastName;",
                                builder)
                        .build();
        RelNode after1 = optimizer.optimize(before1, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(id=[p2.id], firstName=[p2.firstName], lastName=[p2.lastName],"
                    + " isAppend=[false])\n"
                    + "  LogicalJoin(condition=[=(p2.id, c1.creator_id)], joinType=[inner])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[p2], startAlias=[p1], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[p1], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 933)])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[c1], opt=[START], physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, COMMENT,"
                    + " PLACE)]], alias=[_], startAlias=[pl1], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[PLACE]}],"
                    + " alias=[pl1], opt=[VERTEX], uniqueKeyFilters=[=(_.name, _UTF-8'India')])",
                after1.explain().trim());
    }
}
