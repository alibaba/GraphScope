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

public class CBOTest {
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
                Utils.mockIrMeta(
                        "schema/ldbc_schema_exp_hierarchy.json",
                        "statistics/ldbc30_hierarchy_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void Q1_test() {
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
    }

    @Test
    public void Q2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (person1:PERSON)-[:LIKES]->(message:COMMENT|POST), \n"
                                    + "\t   (message:COMMENT|POST)-[:HASCREATOR]->(person2:PERSON),"
                                    + " \n"
                                    + "\t   (person1:PERSON)<-[:HASMODERATOR]-(place:FORUM), \n"
                                    + "     (person2:PERSON)<-[:HASMODERATOR]-(place:FORUM)\n"
                                    + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(message, message)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[message], startAlias=[person1], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-1649429364]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[message], startAlias=[person2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-1649429364]])\n"
                    + "common#-1649429364:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMODERATOR]}],"
                    + " alias=[person1], startAlias=[place], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMODERATOR]}],"
                    + " alias=[place], startAlias=[person2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void Q3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (person1:PERSON)<-[:HASCREATOR]-(comment:COMMENT), \n"
                                        + "\t  (comment:COMMENT)-[:REPLYOF]->(post:POST),\n"
                                        + "\t  (post:POST)<-[:CONTAINEROF]-(forum:FORUM),\n"
                                        + "\t  (forum:FORUM)-[:HASMEMBER]->(person2:PERSON)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[person2], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[person1], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT, POST)]],"
                    + " alias=[comment], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[post], startAlias=[forum], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void Q4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (forum:FORUM)-[:CONTAINEROF]->(post:POST),\n"
                                        + "\t  (forum:FORUM)-[:HASMEMBER]->(person1:PERSON), \n"
                                        + "\t  (forum:FORUM)-[:HASMEMBER]->(person2:PERSON), \n"
                                        + "    (person1:PERSON)-[:KNOWS]->(person2:PERSON), \n"
                                        + "\t  (person1:PERSON)-[:LIKES]->(post:POST),\n"
                                        + "\t  (person2:PERSON)-[:LIKES]->(post:POST)\n"
                                        + "Return count(person1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(person1, person1)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER, INNER]], outerJoinConditions=[[NULL, NULL,"
                    + " NULL]], projFields=[[ALL, ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[person1], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#803682572]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person1], startAlias=[person2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#803682572]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[person1], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#803682572]])\n"
                    + "common#803682572:\n"
                    + "MultiJoin(joinFilter=[=(person2, person2)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[person2], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-1025398524]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[person2], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-1025398524]])\n"
                    + "common#-1025398524:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[CONTAINEROF]}],"
                    + " alias=[post], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }
}
