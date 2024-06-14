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
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class STPathTest {

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
                                "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                        + " ExtendIntersectRule, JoinDecompositionRule,"
                                        + " ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc30_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void st_path_person_id_knows_person_id() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a:PERSON {id: 1})-[c:KNOWS*6..7 {creationDate:"
                                        + " 2012}]->(b:PERSON {id:2}) Return c",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        PhysicalBuilder physicalBuilder =
                new GraphRelProtoPhysicalBuilder(configs, irMeta, new LogicalPlan(after));
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/st_path_test.json"),
                physicalBuilder.build().explain().trim());
    }

    @Test
    public void st_path_between_person_and_place() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a:PERSON{id:933})-[c*2..3]->(b:PLACE{id:999}) Return"
                                        + " count(a);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[a],"
                    + " aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PLACE]}], alias=[b],"
                    + " fusedFilter=[[=(_.id, 999)]], opt=[END])\n"
                    + "    GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[[EdgeLabel(ISPARTOF,"
                    + " PLACE, PLACE), EdgeLabel(ISLOCATEDIN, PERSON, PLACE), EdgeLabel(KNOWS,"
                    + " PERSON, PERSON), EdgeLabel(LIKES, PERSON, COMMENT), EdgeLabel(LIKES,"
                    + " PERSON, POST), EdgeLabel(STUDYAT, PERSON, ORGANISATION), EdgeLabel(WORKAT,"
                    + " PERSON, ORGANISATION), EdgeLabel(ISLOCATEDIN, COMMENT, PLACE),"
                    + " EdgeLabel(ISLOCATEDIN, POST, PLACE), EdgeLabel(ISLOCATEDIN, ORGANISATION,"
                    + " PLACE)]], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON, POST,"
                    + " ORGANISATION, PLACE, COMMENT]}], alias=[_], opt=[END])\n"
                    + "], offset=[2], fetch=[1], path_opt=[ARBITRARY], result_opt=[ALL_V_E],"
                    + " alias=[c], start_alias=[a])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[a], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 933)])",
                after.explain().trim());
    }
}
