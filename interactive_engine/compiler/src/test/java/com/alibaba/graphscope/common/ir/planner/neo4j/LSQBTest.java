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

public class LSQBTest {
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
                                "FilterIntoJoinRule, FilterMatchRule, JoinDecompositionRule,"
                                        + " ExtendIntersectRule, , ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc1_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void lsqb1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (forum:FORUM)-[:HASMEMBER]->(person:PERSON),\n"
                                        + "      (person:PERSON)-[:ISLOCATEDIN]->(city:PLACE),\n"
                                        + "      (city:PLACE)-[:ISPARTOF]->(country:PLACE),\n"
                                        + "      (forum:FORUM)-[:CONTAINEROF]->(post:POST),\n"
                                        + "      (post:POST)<-[:REPLYOF]-(comment:COMMENT),\n"
                                        + "      (comment:COMMENT)-[:HASTAG]->(tag:TAG),\n"
                                        + "      (tag:TAG)-[:HASTYPE]->(tagClass:TAGCLASS)\n"
                                        + "RETURN COUNT(forum);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[forum], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]]): rowcount = 1.0\n"
                    + "  LogicalJoin(condition=[=(forum, forum)], joinType=[inner]): rowcount ="
                    + " 2.3591521409700394E7\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTYPE]}],"
                    + " alias=[tagClass], startAlias=[tag], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 1329914.1776627558\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[tag], startAlias=[comment], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 1329914.1776627558\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT,"
                    + " POST)]], alias=[comment], startAlias=[post], opt=[IN],"
                    + " physicalOpt=[VERTEX]): rowcount = 1011420.0\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[post], startAlias=[forum], opt=[OUT],"
                    + " physicalOpt=[VERTEX]): rowcount = 1003605.0\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX]): rowcount = 90492.0\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[forum], startAlias=[person], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 1605249.4147843944\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[ISPARTOF]}],"
                    + " alias=[country], startAlias=[city], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 9851.375770020533\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " PLACE)]], alias=[city], startAlias=[person], opt=[OUT],"
                    + " physicalOpt=[VERTEX]): rowcount = 9892.0\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX]): rowcount = 9892.0",
                com.alibaba.graphscope.common.ir.tools.Utils.explain(
                                after, SqlExplainLevel.NON_COST_ATTRIBUTES)
                        .trim());
    }

    @Test
    public void lsqb2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p1:PERSON)-[:KNOWS]-(p2:PERSON),\n"
                                        + "      (p1:PERSON)<-[:HASCREATOR]-(c:COMMENT),\n"
                                        + "      (p2:PERSON)<-[:HASCREATOR]-(p:POST),\n"
                                        + "      (c:COMMENT)-[:REPLYOF]->(p:POST)\n"
                                        + "RETURN COUNT(p1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[p1],"
                    + " aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  LogicalJoin(condition=[AND(=(p1, p1), =(p2, p2))], joinType=[inner])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[p2], startAlias=[p1], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[p1], opt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[p1], startAlias=[c], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT, POST)]],"
                    + " alias=[c], startAlias=[p], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON)]], alias=[p2], startAlias=[p], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[p], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void lsqb3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person1:PERSON)-[:KNOWS]-(person2:PERSON),\n"
                                        + "      (person1:PERSON)-[:KNOWS]-(person3:PERSON),\n"
                                        + "      (person2:PERSON)-[:KNOWS]-(person3:PERSON),\n"
                                        + "      (person1:PERSON)-[:ISLOCATEDIN]->(city1:PLACE),\n"
                                        + "      (city1:PLACE)-[:ISPARTOF]->(country:PLACE),\n"
                                        + "      (person2:PERSON)-[:ISLOCATEDIN]->(city2:PLACE),\n"
                                        + "      (city2:PLACE)-[:ISPARTOF]->(country:PLACE),\n"
                                        + "      (person3:PERSON)-[:ISLOCATEDIN]->(city3:PLACE),\n"
                                        + "      (city3:PLACE)-[:ISPARTOF]->(country:PLACE)\n"
                                        + "Where country.name='India'\n"
                                        + "RETURN COUNT(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]]): rowcount = 1.0\n"
                    + "  MultiJoin(joinFilter=[=(person1, person1)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER, INNER]], outerJoinConditions=[[NULL, NULL,"
                    + " NULL]], projFields=[[ALL, ALL, ALL]]): rowcount = 1.0\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[START], physicalOpt=[ITSELF]): rowcount ="
                    + " 6.770704996577686\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " PLACE)]], alias=[_], startAlias=[city1], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 2110.5503080082135\n"
                    + "        CommonTableScan(table=[[common#144032319]]): rowcount = 1.0\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person1], startAlias=[person2], opt=[BOTH], physicalOpt=[VERTEX]):"
                    + " rowcount = 1.0\n"
                    + "      CommonTableScan(table=[[common#144032319]]): rowcount = 1.0\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person1], startAlias=[person3], opt=[BOTH], physicalOpt=[VERTEX]):"
                    + " rowcount = 1.0\n"
                    + "      CommonTableScan(table=[[common#144032319]]): rowcount = 1.0\n"
                    + "common#144032319:\n"
                    + "MultiJoin(joinFilter=[=(person3, person3)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]]): rowcount = 1.0\n"
                    + "  GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person3], opt=[START], physicalOpt=[ITSELF]): rowcount ="
                    + " 45.27996846027078\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " PLACE)]], alias=[_], startAlias=[city2], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 14114.579121189183\n"
                    + "      CommonTableScan(table=[[common#-659026580]]): rowcount ="
                    + " 6.770704996577686\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person3], startAlias=[person2], opt=[BOTH], physicalOpt=[VERTEX]):"
                    + " rowcount = 1.0\n"
                    + "    CommonTableScan(table=[[common#-659026580]]): rowcount ="
                    + " 6.770704996577686\n"
                    + "common#-659026580:\n"
                    + "GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[START], physicalOpt=[ITSELF]): rowcount ="
                    + " 6.770704996577686\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, PLACE)]],"
                    + " alias=[_], startAlias=[city1], opt=[IN], physicalOpt=[VERTEX]): rowcount ="
                    + " 2110.5503080082135\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[ISPARTOF]}],"
                    + " alias=[city3], startAlias=[country], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 1.0\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[ISPARTOF]}],"
                    + " alias=[city1], startAlias=[country], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 1.0\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[ISPARTOF]}],"
                    + " alias=[city2], startAlias=[country], opt=[IN], physicalOpt=[VERTEX]):"
                    + " rowcount = 1.0\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PLACE]}],"
                    + " alias=[country], opt=[VERTEX], uniqueKeyFilters=[=(_.name,"
                    + " _UTF-8'India')]): rowcount = 1.0",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(
                                after, SqlExplainLevel.NON_COST_ATTRIBUTES)
                        .trim());
    }

    @Test
    public void lsqb4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (message:COMMENT)-[:HASTAG]->(tag:TAG),\n"
                                    + "      (message:COMMENT)-[:HASCREATOR]->(creator:PERSON),\n"
                                    + "      (liker:PERSON)-[:LIKES]->(message:COMMENT),\n"
                                    + "      (comment:COMMENT)-[:REPLYOF]->(message:COMMENT)\n"
                                    + "RETURN COUNT(message);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[tag], startAlias=[message], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[creator], startAlias=[message], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, COMMENT)]],"
                    + " alias=[liker], startAlias=[message], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT,"
                    + " COMMENT)]], alias=[comment], startAlias=[message], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[message], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void lsqb5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (message:COMMENT)-[:HASTAG]->(tag1:TAG),\n"
                                        + "      (comment:COMMENT)-[:HASTAG]->(tag2:TAG),\n"
                                        + "      (comment:COMMENT)-[:REPLYOF]->(message:COMMENT)\n"
                                        + "      WHERE tag1<>tag2\n"
                                        + "RETURN COUNT(message);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        // print estimated count with other attributes
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]]): rowcount = 1.0\n"
                    + "  LogicalFilter(condition=[<>(tag1, tag2)]): rowcount = 899705.0620204923\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[tag2], startAlias=[comment], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 1799410.1240409845\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[tag1], startAlias=[message], opt=[OUT], physicalOpt=[VERTEX]):"
                    + " rowcount = 1368478.8223372442\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT,"
                    + " COMMENT)]], alias=[comment], startAlias=[message], opt=[IN],"
                    + " physicalOpt=[VERTEX]): rowcount = 1040749.0000000001\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[message], opt=[VERTEX]): rowcount = 2052169.0",
                com.alibaba.graphscope.common.ir.tools.Utils.explain(
                                after, SqlExplainLevel.NON_COST_ATTRIBUTES)
                        .trim());
    }

    @Test
    public void lsqb6_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person3:PERSON)-[:HASINTEREST]->(tag:TAG),\n"
                                        + "      (person1:PERSON)-[:KNOWS]->(person2:PERSON),\n"
                                        + "      (person2:PERSON)-[:KNOWS]->(person3:PERSON)\n"
                                        + "WHERE person1<>person3\n"
                                        + "RETURN COUNT(person3);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person3], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  LogicalFilter(condition=[<>(person1, person3)])\n"
                    + "    LogicalJoin(condition=[=(person3, person3)], joinType=[inner])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[tag], startAlias=[person3], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person3], opt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person1], startAlias=[person2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person3], startAlias=[person2], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[VERTEX])",
                after.explain().trim());
    }
}
