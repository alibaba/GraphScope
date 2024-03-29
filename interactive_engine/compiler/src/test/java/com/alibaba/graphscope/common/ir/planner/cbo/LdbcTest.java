package com.alibaba.graphscope.common.ir.planner.cbo;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LdbcTest {
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
                                        + " ExpandGetVFusionRule",
                                "graph.planner.cbo.glogue.schema",
                                "target/test-classes/statistics/ldbc30_hierarchy_statistics.txt"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta = Utils.mockSchemaMeta("schema/ldbc_schema_exp_hierarchy.json");
    }

    @Test
    public void ldbc_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p: PERSON{id: 1939})-[k:KNOWS*1..4]-(f:"
                                        + " PERSON)-[:ISLOCATEDIN]->(city),\n"
                                        + "     "
                                        + " (f)-[:WORKAT]->(:COMPANY)-[:ISLOCATEDIN]->(:COUNTRY),\n"
                                        + "     "
                                        + " (f)-[:STUDYAT]->(:UNIVERSITY)-[:ISLOCATEDIN]->(:CITY)\n"
                                        + "WHERE f.firstName = \"Mikhail\" AND f.id <> 1939\n"
                                        + "WITH f, length(k) as len\n"
                                        + "RETURN f, min(len) as distance\n"
                                        + "ORDER BY distance ASC, f.lastName ASC, f.id ASC\n"
                                        + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[distance], sort1=[f.lastName], sort2=[f.id], dir0=[ASC],"
                    + " dir1=[ASC], dir2=[ASC], fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[f], aliases=[f]}],"
                    + " values=[[{operands=[len], aggFunction=MIN, alias='distance',"
                    + " distinct=false}]])\n"
                    + "    GraphLogicalProject(f=[f], len=[k.~len], isAppend=[false])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[ISLOCATEDIN]}],"
                    + " alias=[_], startAlias=[PATTERN_VERTEX$5], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[WORKAT]}],"
                    + " alias=[PATTERN_VERTEX$5], startAlias=[f], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISLOCATEDIN]}], alias=[city], startAlias=[f], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISLOCATEDIN]}], alias=[_], startAlias=[PATTERN_VERTEX$9],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[STUDYAT]}], alias=[PATTERN_VERTEX$9], startAlias=[f], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[f], fusedFilter=[[AND(=(_.firstName,"
                    + " _UTF-8'Mikhail'), <>(_.id, 1939))]], opt=[END])\n"
                    + "                 "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[3], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[k], start_alias=[p])\n"
                    + "                    GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[p], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " 1939)])",
                after.explain().trim());
    }

    @Test
    public void ldbc2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:PERSON{id:"
                                    + " 1939})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message :"
                                    + " POST | COMMENT)\n"
                                    + "WHERE message.creationDate < 20130301000000000\n"
                                    + "return\n"
                                    + "    friend.id AS personId,\n"
                                    + "    friend.firstName AS personFirstName,\n"
                                    + "  friend.lastName AS personLastName,\n"
                                    + "  message.id AS postOrCommentId,\n"
                                    + "  message.content AS content,\n"
                                    + "  message.imageFile AS imageFile,\n"
                                    + "  message.creationDate AS postOrCommentCreationDate\n"
                                    + "ORDER BY\n"
                                    + "  postOrCommentCreationDate DESC,\n"
                                    + "  postOrCommentId ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[postOrCommentCreationDate], sort1=[postOrCommentId],"
                    + " dir0=[DESC], dir1=[ASC], fetch=[20])\n"
                    + "  GraphLogicalProject(personId=[friend.id],"
                    + " personFirstName=[friend.firstName], personLastName=[friend.lastName],"
                    + " postOrCommentId=[message.id], content=[message.content],"
                    + " imageFile=[message.imageFile],"
                    + " postOrCommentCreationDate=[message.creationDate], isAppend=[false])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], fusedFilter=[[<(_.creationDate, 20130301000000000)]],"
                    + " opt=[START], physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[_], startAlias=[friend], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[friend], startAlias=[p], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[p], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 1939)])",
                after.explain().trim());
    }

    @Test
    public void ldbc3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (countryX:COUNTRY {name:"
                                    + " 'Laos'})<-[:ISLOCATEDIN]-(messageX)-[:HASCREATOR]->(otherP:PERSON),\n"
                                    + "       (countryY:COUNTRY {name:"
                                    + " 'United_States'})<-[:ISLOCATEDIN]-(messageY)-[:HASCREATOR]->(otherP:PERSON),\n"
                                    + "      "
                                    + " (otherP)-[:ISLOCATEDIN]->(city)-[:ISPARTOF]->(countryCity),\n"
                                    + "       (person:PERSON {id:1939})-[:KNOWS*1..3]-(otherP)\n"
                                    + "WHERE countryCity.name <> 'Laos' AND countryCity.name <>"
                                    + " 'United_States'\n"
                                    + "      AND messageX.creationDate >= 20100505013715278 and"
                                    + " messageX.creationDate <= 20130604130807720\n"
                                    + "      AND messageY.creationDate >= 20100505013715278 and"
                                    + " messageY.creationDate <= 20130604130807720\n"
                                    + "WITH otherP, count(messageX) as xCount, count(messageY) as"
                                    + " yCount\n"
                                    + "RETURN otherP.id as id,\n"
                                    + "             otherP.firstName as firstName,\n"
                                    + "             otherP.lastName as lastName,\n"
                                    + "             xCount,\n"
                                    + "             yCount,\n"
                                    + "             xCount + yCount as total\n"
                                    + "ORDER BY total DESC, id ASC\n"
                                    + "Limit 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[total], sort1=[id], dir0=[DESC], dir1=[ASC], fetch=[20])\n"
                    + "  GraphLogicalProject(id=[otherP.id], firstName=[otherP.firstName],"
                    + " lastName=[otherP.lastName], xCount=[xCount], yCount=[yCount],"
                    + " total=[+(xCount, yCount)], isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[otherP], aliases=[otherP]}],"
                    + " values=[[{operands=[messageX], aggFunction=COUNT, alias='xCount',"
                    + " distinct=false}, {operands=[messageY], aggFunction=COUNT, alias='yCount',"
                    + " distinct=false}]])\n"
                    + "      GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COUNTRY]}],"
                    + " alias=[countryX], fusedFilter=[[=(_.name, _UTF-8'Laos')]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISLOCATEDIN]}], alias=[_], startAlias=[messageX], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[messageX], fusedFilter=[[AND(>=(_.creationDate,"
                    + " 20100505013715278), <=(_.creationDate, 20130604130807720))]], opt=[START],"
                    + " physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[_], startAlias=[otherP], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[COUNTRY]}], alias=[countryY], fusedFilter=[[=(_.name,"
                    + " _UTF-8'United_States')]], opt=[END], physicalOpt=[ITSELF])\n"
                    + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISLOCATEDIN]}], alias=[_], startAlias=[messageY], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                  GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[messageY], fusedFilter=[[AND(>=(_.creationDate,"
                    + " 20100505013715278), <=(_.creationDate, 20130604130807720))]], opt=[START],"
                    + " physicalOpt=[ITSELF])\n"
                    + "                    GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[_], startAlias=[otherP], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                      GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[COUNTRY]}], alias=[countryCity], fusedFilter=[[AND(<>(_.name,"
                    + " _UTF-8'Laos'), <>(_.name, _UTF-8'United_States'))]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "                        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISPARTOF]}], alias=[_], startAlias=[city], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISLOCATEDIN]}], alias=[city], startAlias=[otherP], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                            GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[otherP], opt=[END])\n"
                    + "                             "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[person])\n"
                    + "                               "
                    + " GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 1939)])",
                after.explain().trim());
    }

    @Test
    public void ldbc4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id: 168944})-[:KNOWS]-(friend:PERSON),\n"
                                    + "      (friend)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag)\n"
                                    + "Where post.creationDate >= 20100111014617581 AND"
                                    + " post.creationDate <= 20130604130807720\n"
                                    + "Return count(person);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTAG]}],"
                    + " alias=[tag], startAlias=[post], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], fusedFilter=[[AND(>=(_.creationDate, 20100111014617581),"
                    + " <=(_.creationDate, 20130604130807720))]], opt=[START],"
                    + " physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[_], startAlias=[friend], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[friend], startAlias=[person], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 168944)])",
                after.explain().trim());
    }

    @Test
    public void ldbc5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON"
                                    + " {id:2199023378950})-[k:KNOWS*1..3]-(other)<-[hasMember:HASMEMBER]-(forum:FORUM),\n"
                                    + "     "
                                    + " (other)<-[:HASCREATOR]-(post:POST)<-[:CONTAINEROF]-(forum)\n"
                                    + "RETURN forum.title as title, forum.id as id, count(distinct"
                                    + " post) AS postCount\n"
                                    + "ORDER BY\n"
                                    + "    postCount DESC,\n"
                                    + "    id ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[postCount], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[forum.title, forum.id],"
                    + " aliases=[title, id]}], values=[[{operands=[post], aggFunction=COUNT,"
                    + " alias='postCount', distinct=true}]])\n"
                    + "    MultiJoin(joinFilter=[=(forum, forum)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[CONTAINEROF]}],"
                    + " alias=[forum], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-1186040689]])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[START])\n"
                    + "        GraphLogicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[hasMember], startAlias=[other], opt=[IN])\n"
                    + "          CommonTableScan(table=[[common#-1186040689]])\n"
                    + "common#-1186040689:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[post], startAlias=[other], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[other], opt=[END])\n"
                    + "    GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[k], start_alias=[person])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 2199023378950)])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void ldbc6_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON"
                                    + " {id:2199023382370})-[:KNOWS*1..3]-(other)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:TAG"
                                    + " {name: \"North_German_Confederation\"}),\n"
                                    + "      (post)-[:HASTAG]->(otherTag:TAG)\n"
                                    + "WHERE otherTag <> tag\n"
                                    + "RETURN otherTag.name as name, count(post) as postCnt\n"
                                    + "ORDER BY postCnt desc, name asc\n"
                                    + "LIMIT 10;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[postCnt], sort1=[name], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[10])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[otherTag.name], aliases=[name]}],"
                    + " values=[[{operands=[post], aggFunction=COUNT, alias='postCnt',"
                    + " distinct=false}]])\n"
                    + "    LogicalFilter(condition=[<>(otherTag, tag)])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTAG]}],"
                    + " alias=[otherTag], startAlias=[post], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, _UTF-8'North_German_Confederation')]],"
                    + " opt=[END], physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTAG]}],"
                    + " alias=[_], startAlias=[post], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[post], startAlias=[other], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[other], opt=[END])\n"
                    + "               "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[person])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " 2199023382370)])",
                after.explain().trim());
    }

    // todo: fix issues in ldbc7: expand (with alias) + getV cannot be fused thus causing the
    // execution errors of extend intersect
    @Test
    public void ldbc7_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " 2199023382370})<-[:HASCREATOR]-(message)<-[like:LIKES]-(liker:PERSON),\n"
                                    + "      (liker)-[:KNOWS]-(person)\n"
                                    + "WITH liker, message, like.creationDate AS likeTime, person\n"
                                    + "ORDER BY likeTime DESC, message.id ASC\n"
                                    + "WITH liker, person, head(collect(message)) as message,"
                                    + " head(collect(likeTime)) AS likeTime\n"
                                    + "RETURN\n"
                                    + "    liker.id AS personId,\n"
                                    + "    liker.firstName AS personFirstName,\n"
                                    + "    liker.lastName AS personLastName,\n"
                                    + "    likeTime AS likeCreationDate,\n"
                                    + "    message.id AS commentOrPostId\n"
                                    + "ORDER BY\n"
                                    + "    likeCreationDate DESC,\n"
                                    + "    personId ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[likeCreationDate], sort1=[personId], dir0=[DESC],"
                    + " dir1=[ASC], fetch=[20])\n"
                    + "  GraphLogicalProject(personId=[liker.id],"
                    + " personFirstName=[liker.firstName], personLastName=[liker.lastName],"
                    + " likeCreationDate=[likeTime], commentOrPostId=[message.id],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[liker, person], aliases=[liker,"
                    + " person]}], values=[[{operands=[message], aggFunction=FIRST_VALUE,"
                    + " alias='message', distinct=false}, {operands=[likeTime],"
                    + " aggFunction=FIRST_VALUE, alias='likeTime', distinct=false}]])\n"
                    + "      GraphLogicalSort(sort0=[likeTime], sort1=[message.id], dir0=[DESC],"
                    + " dir1=[ASC])\n"
                    + "        GraphLogicalProject(liker=[liker], message=[message],"
                    + " likeTime=[like.creationDate], person=[person], isAppend=[false])\n"
                    + "          MultiJoin(joinFilter=[=(liker, liker)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "            GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[liker], opt=[START])\n"
                    + "              GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[LIKES]}], alias=[like], startAlias=[message], opt=[IN])\n"
                    + "                CommonTableScan(table=[[common#-1625147595]])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[liker], startAlias=[person], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "              CommonTableScan(table=[[common#-1625147595]])\n"
                    + "common#-1625147595:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[message], startAlias=[person], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 2199023382370)])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void ldbc8_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " 2199023382370})<-[:HASCREATOR]-(message)<-[:REPLYOF]-(comment:COMMENT)-[:HASCREATOR]->(author:PERSON)\n"
                                    + "RETURN\n"
                                    + "    author.id,\n"
                                    + "    author.firstName,\n"
                                    + "    author.lastName,\n"
                                    + "    comment.creationDate as commentDate,\n"
                                    + "    comment.id as commentId,\n"
                                    + "    comment.content\n"
                                    + "ORDER BY\n"
                                    + "    commentDate desc,\n"
                                    + "    commentId asc\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[commentDate], sort1=[commentId], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "  GraphLogicalProject(id=[author.id], firstName=[author.firstName],"
                    + " lastName=[author.lastName], commentDate=[comment.creationDate],"
                    + " commentId=[comment.id], content=[comment.content], isAppend=[false])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[author], startAlias=[comment], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[REPLYOF]}],"
                    + " alias=[comment], startAlias=[message], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[message], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 2199023382370)])",
                after.explain().trim());
    }

    @Test
    public void ldbc9_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " 2199023382370})-[:KNOWS*1..3]-(friend:PERSON)<-[:HASCREATOR]-(message)\n"
                                    + "WHERE friend <> person\n"
                                    + "    and message.creationDate < 20130301000000000\n"
                                    + "RETURN\n"
                                    + "  friend.id AS personId,\n"
                                    + "  friend.firstName AS personFirstName,\n"
                                    + "  friend.lastName AS personLastName,\n"
                                    + "  message.id AS commentOrPostId,\n"
                                    + "  message.creationDate AS commentOrPostCreationDate\n"
                                    + "ORDER BY\n"
                                    + "  commentOrPostCreationDate DESC,\n"
                                    + "  commentOrPostId ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[commentOrPostCreationDate], sort1=[commentOrPostId],"
                    + " dir0=[DESC], dir1=[ASC], fetch=[20])\n"
                    + "  GraphLogicalProject(personId=[friend.id],"
                    + " personFirstName=[friend.firstName], personLastName=[friend.lastName],"
                    + " commentOrPostId=[message.id],"
                    + " commentOrPostCreationDate=[message.creationDate], isAppend=[false])\n"
                    + "    LogicalFilter(condition=[<>(friend, person)])\n"
                    + "      GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], fusedFilter=[[<(_.creationDate, 20130301000000000)]],"
                    + " opt=[START], physicalOpt=[ITSELF])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[_], startAlias=[friend], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[friend], opt=[END])\n"
                    + "           "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[person])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " 2199023382370)])",
                after.explain().trim());
    }

    @Test
    public void ldbc11_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " 2199023382370})-[:KNOWS*1..3]-(friend:PERSON)-[workAt:WORKAT]->(company:COMPANY)-[:ISLOCATEDIN]->(:COUNTRY"
                                    + " {name: \"Kazakhstan\"})\n"
                                    + "WHERE person <> friend\n"
                                    + "    and    workAt.workFrom < 2013\n"
                                    + "RETURN DISTINCT\n"
                                    + "  friend.id AS personId,\n"
                                    + "  friend.firstName AS personFirstName,\n"
                                    + "  friend.lastName AS personLastName,\n"
                                    + "  company.name AS organizationName,\n"
                                    + "  workAt.workFrom AS organizationWorkFromYear\n"
                                    + "ORDER BY\n"
                                    + "  organizationWorkFromYear ASC,\n"
                                    + "  personId ASC,\n"
                                    + "  organizationName DESC\n"
                                    + "LIMIT 10;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[organizationWorkFromYear], sort1=[personId],"
                    + " sort2=[organizationName], dir0=[ASC], dir1=[ASC], dir2=[DESC],"
                    + " fetch=[10])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[friend.id, friend.firstName,"
                    + " friend.lastName, company.name, workAt.workFrom], aliases=[personId,"
                    + " personFirstName, personLastName, organizationName,"
                    + " organizationWorkFromYear]}], values=[[]])\n"
                    + "    LogicalFilter(condition=[<>(person, friend)])\n"
                    + "      GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COUNTRY]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, _UTF-8'Kazakhstan')]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISLOCATEDIN]}], alias=[_], startAlias=[company], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalGetV(tableConfig=[{isAll=false, tables=[COMPANY]}],"
                    + " alias=[company], opt=[END])\n"
                    + "            GraphLogicalExpand(tableConfig=[{isAll=false, tables=[WORKAT]}],"
                    + " alias=[workAt], startAlias=[friend], fusedFilter=[[<(_.workFrom, 2013)]],"
                    + " opt=[OUT])\n"
                    + "              GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[friend], opt=[END])\n"
                    + "               "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[person])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " 2199023382370)])",
                after.explain().trim());
    }

    @Test
    public void ldbc12_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (:PERSON {id:"
                                    + " 2199023382370})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comment:COMMENT)-[:REPLYOF]->(:POST)-[:HASTAG]->(tag:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF*0..7]->(baseTagClass:TAGCLASS"
                                    + " {name: \"Organisation\"})\n"
                                    + "RETURN\n"
                                    + "  friend.id AS personId,\n"
                                    + "  friend.firstName AS personFirstName,\n"
                                    + "  friend.lastName AS personLastName,\n"
                                    + "  collect(DISTINCT tag.name) AS tagNames,\n"
                                    + "  count(DISTINCT comment) AS replyCount\n"
                                    + "ORDER BY\n"
                                    + "  replyCount DESC,\n"
                                    + "  personId ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[replyCount], sort1=[personId], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[friend.id, friend.firstName,"
                    + " friend.lastName], aliases=[personId, personFirstName, personLastName]}],"
                    + " values=[[{operands=[tag.name], aggFunction=COLLECT, alias='tagNames',"
                    + " distinct=true}, {operands=[comment], aggFunction=COUNT, alias='replyCount',"
                    + " distinct=true}]])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[TAGCLASS]}],"
                    + " alias=[baseTagClass], fusedFilter=[[=(_.name, _UTF-8'Organisation')]],"
                    + " opt=[END])\n"
                    + "     "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISSUBCLASSOF]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "], fetch=[7], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[PATTERN_VERTEX$9])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTYPE]}],"
                    + " alias=[PATTERN_VERTEX$9], startAlias=[tag], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTAG]}],"
                    + " alias=[tag], startAlias=[PATTERN_VERTEX$5], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[PATTERN_VERTEX$5], startAlias=[comment],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[comment], startAlias=[friend], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[friend], startAlias=[PATTERN_VERTEX$0],"
                    + " opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[PATTERN_VERTEX$0], opt=[VERTEX],"
                    + " uniqueKeyFilters=[=(_.id, 2199023382370)])",
                after.explain().trim());
    }
}
