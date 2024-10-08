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

package com.alibaba.graphscope.common.ir.planner.neo4j;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CBOTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @Before
    public void beforeTest() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.join.min.pattern.size",
                                "3",
                                "graph.planner.label.constraints.enabled",
                                "true",
                                "graph.planner.join.by.edge.enabled",
                                "true",
                                "graph.planner.intersect.cost.factor",
                                "3",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                        + " JoinDecompositionRule, ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc1_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void CBO_1_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (message:POST)-[:HASCREATOR]->(person),\n"
                                        + "      (message:POST)-[:HASTAG]->(tag:TAG),\n"
                                        + "      (person)-[:HASINTEREST]->(tag:TAG)\n"
                                        + "Return count(person);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]]): rowcount = 1.0, cumulative cost = {2670207.542222045"
                    + " rows, 2669181.0 cpu, 0.0 io}\n"
                    + "  LogicalJoin(condition=[AND(=(person, person), =(tag, tag))],"
                    + " joinType=[inner]): rowcount = 1027.5422220449734, cumulative cost ="
                    + " {2670206.542222045 rows, 2669181.0 cpu, 0.0 io}\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[tag], startAlias=[person], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 229166.0, cumulative cost = {239058.0 rows, 239059.0 cpu, 0.0"
                    + " io}\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX]): rowcount = 9892.0, cumulative cost = {9892.0"
                    + " rows, 9893.0 cpu, 0.0 io}\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST, PERSON)]],"
                    + " alias=[person], startAlias=[message], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 713258.0, cumulative cost = {2430121.0 rows, 2430122.0 cpu, 0.0"
                    + " io}\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST, TAG)]],"
                    + " alias=[tag], startAlias=[message], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 713258.0, cumulative cost = {1716863.0 rows, 1716864.0 cpu, 0.0"
                    + " io}\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[message], opt=[VERTEX]): rowcount = 1003605.0, cumulative cost ="
                    + " {1003605.0 rows, 1003606.0 cpu, 0.0 io}",
                com.alibaba.graphscope.common.ir.tools.Utils.explain(
                                after, SqlExplainLevel.ALL_ATTRIBUTES)
                        .trim());
    }

    @Test
    public void CBO_1_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (message)-[:KNOWS|HASMODERATOR]->(person:PERSON),\n"
                                        + "      (message)-[:HASTAG|HASINTEREST]->(tag:TAG),\n"
                                        + "      (person)-[:HASINTEREST]->(tag:TAG)\n"
                                        + "Return count(person);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]]): rowcount = 1.0, cumulative cost = {1977699.124583344"
                    + " rows, 1975603.2326665604 cpu, 0.0 io}\n"
                    + "  LogicalJoin(condition=[AND(=(person, person), =(tag, tag))],"
                    + " joinType=[inner]): rowcount = 2096.8919167835015, cumulative cost ="
                    + " {1977698.124583344 rows, 1975603.2326665604 cpu, 0.0 io}\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[tag], startAlias=[person], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 229166.0, cumulative cost = {239058.0 rows, 239059.0 cpu, 0.0"
                    + " io}\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX]): rowcount = 9892.0, cumulative cost = {9892.0"
                    + " rows, 9893.0 cpu, 0.0 io}\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASINTEREST, PERSON, TAG),"
                    + " EdgeLabel(HASTAG, FORUM, TAG)]], alias=[tag], startAlias=[message],"
                    + " opt=[OUT], physicalOpt=[VERTEX]): rowcount = 1455536.2326665604, cumulative"
                    + " cost = {1736543.2326665604 rows, 1736544.2326665604 cpu, 0.0 io}\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS,"
                    + " HASMODERATOR]}], alias=[message], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX]): rowcount = 271115.0, cumulative cost = {281007.0"
                    + " rows, 281008.0 cpu, 0.0 io}\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX]): rowcount = 9892.0, cumulative cost = {9892.0"
                    + " rows, 9893.0 cpu, 0.0 io}",
                com.alibaba.graphscope.common.ir.tools.Utils.explain(
                                after, SqlExplainLevel.ALL_ATTRIBUTES)
                        .trim());
    }

    @Test
    public void CBO_2_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (person1:PERSON)-[:LIKES]->(message:POST),\n"
                                        + "      (message:POST)<-[:CONTAINEROF]-(person2:FORUM),\n"
                                        + "      (person1:PERSON)-[:KNOWS]->(place),\n"
                                        + "      (person2:FORUM)-[:HASMODERATOR]->(place)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  LogicalJoin(condition=[AND(=(person1, person1), =(place, place))],"
                    + " joinType=[inner])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[place], startAlias=[person1], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[person1], startAlias=[message], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[CONTAINEROF]}],"
                    + " alias=[message], startAlias=[person2], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMODERATOR]}], alias=[person2], startAlias=[place], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[place], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void CBO_2_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (person1:PERSON)-[:LIKES]->(message:POST),\n"
                                        + "\t    (message:POST)<-[:CONTAINEROF]-(person2:FORUM),\n"
                                        + "\t    (person1:PERSON)-[:KNOWS|HASINTEREST]->(place),\n"
                                        + "      (person2:FORUM)-[:HASMODERATOR|HASTAG]->(place)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  LogicalJoin(condition=[AND(=(person1, person1), =(place, place))],"
                    + " joinType=[inner])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(KNOWS, PERSON, PERSON),"
                    + " EdgeLabel(HASINTEREST, PERSON, TAG)]], alias=[place], startAlias=[person1],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX])\n"
                    + "    LogicalJoin(condition=[=(person2, person2)], joinType=[inner])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASMODERATOR, FORUM,"
                    + " PERSON), EdgeLabel(HASTAG, FORUM, TAG)]], alias=[place],"
                    + " startAlias=[person2], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[person2], opt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[person1], startAlias=[message], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[message], startAlias=[person2], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[person2], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void CBO_3_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (person1)<-[:HASCREATOR]-(comment:COMMENT),\n"
                                        + "      (comment:COMMENT)-[:REPLYOF]->(post:POST),\n"
                                        + "      (post:POST)<-[:CONTAINEROF]-(forum),\n"
                                        + "      (forum)-[:HASMEMBER]->(person2)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  LogicalJoin(condition=[=(forum, forum)], joinType=[inner])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[person1], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT, POST)]],"
                    + " alias=[comment], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[post], startAlias=[forum], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[forum], startAlias=[person2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void CBO_3_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (p:COMMENT)-[]->(:PERSON)-[]->(:PLACE),\n"
                                        + "      (p)<-[]-(message),\n"
                                        + "      (message)-[]->(tag:TAG)\n"
                                        + "Return count(p);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[p],"
                    + " aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASINTEREST, PERSON, TAG),"
                    + " EdgeLabel(HASTAG, COMMENT, TAG)]], alias=[tag], startAlias=[message],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, COMMENT),"
                    + " EdgeLabel(REPLYOF, COMMENT, COMMENT)]], alias=[message], startAlias=[p],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[p], opt=[START], physicalOpt=[ITSELF])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[_], startAlias=[PATTERN_VERTEX$1], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " PLACE)]], alias=[_], startAlias=[PATTERN_VERTEX$1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[PATTERN_VERTEX$1], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void CBO_4_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (forum)-[:CONTAINEROF]->(post:POST),\n"
                                        + "      (forum)-[:HASMEMBER]->(person1:PERSON),\n"
                                        + "      (forum)-[:HASMEMBER]->(person2:PERSON),\n"
                                        + "      (person1:PERSON)-[:KNOWS]->(person2:PERSON),\n"
                                        + "      (person1:PERSON)-[:LIKES]->(post:POST),\n"
                                        + "      (person2:PERSON)-[:LIKES]->(post:POST)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(forum, forum)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER, INNER]], outerJoinConditions=[[NULL, NULL,"
                    + " NULL]], projFields=[[ALL, ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[CONTAINEROF]}],"
                    + " alias=[forum], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#268739597]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[forum], startAlias=[person1], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#268739597]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[forum], startAlias=[person2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#268739597]])\n"
                    + "common#268739597:\n"
                    + "LogicalJoin(condition=[AND(=(person1, person1), =(person2, person2))],"
                    + " joinType=[inner])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person2], startAlias=[person1], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[person2], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[person1], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void CBO_4_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (forum)-[:HASTAG]->(post:TAG),\n"
                                        + "      (forum)-[:HASMODERATOR]->(person1),\n"
                                        + "      (forum)-[:HASMODERATOR|CONTAINEROF]->(person2),\n"
                                        + "      (person1)-[:KNOWS|LIKES]->(person2),\n"
                                        + "      (person1)-[:HASINTEREST]->(post:TAG),\n"
                                        + "      (person2)-[:HASINTEREST|HASTAG]->(post:TAG)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]]): rowcount = 1.0\n"
                    + "  MultiJoin(joinFilter=[=(person2, person2)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER, INNER]], outerJoinConditions=[[NULL, NULL,"
                    + " NULL]], projFields=[[ALL, ALL, ALL]]): rowcount = 1.0\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASMODERATOR, FORUM,"
                    + " PERSON), EdgeLabel(CONTAINEROF, FORUM, POST)]], alias=[person2],"
                    + " startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX]): rowcount ="
                    + " 5395.508895399443\n"
                    + "      CommonTableScan(table=[[common#1803177513]]): rowcount ="
                    + " 446.2587786663215\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON, POST]}],"
                    + " alias=[person2], opt=[START], physicalOpt=[ITSELF]): rowcount = 1.0\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASINTEREST, PERSON, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[post], opt=[IN],"
                    + " physicalOpt=[VERTEX]): rowcount = 1.5439762993307926\n"
                    + "        CommonTableScan(table=[[common#1803177513]]): rowcount ="
                    + " 446.2587786663215\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON, POST]}],"
                    + " alias=[person2], opt=[END], physicalOpt=[ITSELF]): rowcount = 1.0\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(KNOWS, PERSON, PERSON),"
                    + " EdgeLabel(LIKES, PERSON, POST)]], alias=[_], startAlias=[person1],"
                    + " opt=[OUT], physicalOpt=[VERTEX]): rowcount = 1.0\n"
                    + "        CommonTableScan(table=[[common#1803177513]]): rowcount ="
                    + " 446.2587786663215\n"
                    + "common#1803177513:\n"
                    + "MultiJoin(joinFilter=[=(post, post)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]]): rowcount = 446.2587786663215\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, FORUM, TAG)]],"
                    + " alias=[post], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 309765.99999999994\n"
                    + "    CommonTableScan(table=[[common#114550231]]): rowcount ="
                    + " 90491.99999999999\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[post], startAlias=[person1], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 446.2587786663215\n"
                    + "    CommonTableScan(table=[[common#114550231]]): rowcount ="
                    + " 90491.99999999999\n"
                    + "common#114550231:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMODERATOR]}],"
                    + " alias=[forum], startAlias=[person1], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 90491.99999999999\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX]): rowcount = 9892.0",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(
                                after, SqlExplainLevel.NON_COST_ATTRIBUTES)
                        .trim());
    }
}
