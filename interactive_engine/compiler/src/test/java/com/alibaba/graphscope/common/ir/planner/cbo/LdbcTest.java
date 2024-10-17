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
                                "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule, FlatJoinToExpandRule,"
                                        + " ExtendIntersectRule, ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc30_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void ldbc1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH shortestPath(p: PERSON {id : $personId})"
                                    + " -[k:KNOWS*1..4]-(f:PERSON {firstName : $firstName})\n"
                                    + "where f <> p\n"
                                    + "WITH f, length(k) as distance\n"
                                    + "ORDER  BY distance ASC, f.lastName ASC, f.id ASC\n"
                                    + "LIMIT 20\n"
                                    + "\n"
                                    + "\n"
                                    + "OPTIONAL MATCH (f:"
                                    + " PERSON)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)\n"
                                    + "WITH \n"
                                    + "    f, distance,\n"
                                    + "    CASE\n"
                                    + "        WHEN company is null Then null\n"
                                    + "        ELSE [company.name, workAt.workFrom, country.name]\n"
                                    + "    END as companies\n"
                                    + "WITH f, collect(companies) as company_info, distance\n"
                                    + "\n"
                                    + "OPTIONAL MATCH (f:"
                                    + " PERSON)-[studyAt:STUDYAT]->(university)-[:ISLOCATEDIN]->(universityCity:PLACE)\n"
                                    + "WITH\n"
                                    + "  f, company_info, distance,\n"
                                    + "\tCASE \n"
                                    + "\t\tWHEN university is null Then null\n"
                                    + "\t\tELSE [university.name, studyAt.classYear,"
                                    + " universityCity.name]\n"
                                    + "\tEND as universities\n"
                                    + "WITH f, collect(universities) as university_info ,"
                                    + " company_info, distance\n"
                                    + "\n"
                                    + "MATCH (f:PERSON)-[:ISLOCATEDIN]->(locationCity:PLACE)\n"
                                    + "\n"
                                    + "return f.id AS friendId;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(friendId=[f.id], isAppend=[false])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, PLACE)]],"
                    + " alias=[locationCity], startAlias=[f], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[f, company_info, distance],"
                    + " aliases=[f, company_info, distance]}], values=[[{operands=[universities],"
                    + " aggFunction=COLLECT, alias='university_info', distinct=false}]])\n"
                    + "      GraphLogicalProject(f=[f], company_info=[company_info],"
                    + " distance=[distance], universities=[CASE(IS NULL(university), null:NULL,"
                    + " ARRAY(university.name, studyAt.classYear, universityCity.name))],"
                    + " isAppend=[false])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " ORGANISATION, PLACE)]], alias=[universityCity], opt=[OUT],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "          GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[ORGANISATION]}], alias=[university], opt=[END])\n"
                    + "            GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[STUDYAT]}], alias=[studyAt], startAlias=[f], opt=[OUT],"
                    + " optional=[true])\n"
                    + "              GraphLogicalAggregate(keys=[{variables=[f, distance],"
                    + " aliases=[f, distance]}], values=[[{operands=[companies],"
                    + " aggFunction=COLLECT, alias='company_info', distinct=false}]])\n"
                    + "                GraphLogicalProject(f=[f], distance=[distance],"
                    + " companies=[CASE(IS NULL(company), null:NULL, ARRAY(company.name,"
                    + " workAt.workFrom, country.name))], isAppend=[false])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " ORGANISATION, PLACE)]], alias=[country], opt=[OUT], physicalOpt=[VERTEX],"
                    + " optional=[true])\n"
                    + "                    GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[ORGANISATION]}], alias=[company], opt=[END])\n"
                    + "                      GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[WORKAT]}], alias=[workAt], startAlias=[f], opt=[OUT],"
                    + " optional=[true])\n"
                    + "                        GraphLogicalSort(sort0=[distance],"
                    + " sort1=[f.lastName], sort2=[f.id], dir0=[ASC], dir1=[ASC], dir2=[ASC],"
                    + " fetch=[20])\n"
                    + "                          GraphLogicalProject(f=[f], distance=[k.~len],"
                    + " isAppend=[false])\n"
                    + "                            LogicalFilter(condition=[<>(f, p)])\n"
                    + "                              GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[f], fusedFilter=[[=(_.firstName, ?1)]],"
                    + " opt=[END])\n"
                    + "                               "
                    + " GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[_], opt=[OTHER])\n"
                    + "], offset=[1], fetch=[3], path_opt=[ANY_SHORTEST], result_opt=[ALL_V_E],"
                    + " alias=[k], start_alias=[p])\n"
                    + "                                 "
                    + " GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}], alias=[p],"
                    + " opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])",
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
                                "MATCH \n"
                                    + "    (countryX: PLACE {name: $countryXName"
                                    + " })<-[:ISLOCATEDIN]-(messageX : POST |"
                                    + " COMMENT)-[:HASCREATOR]->(otherP:PERSON),\n"
                                    + "    (countryY: PLACE {name: $countryYName"
                                    + " })<-[:ISLOCATEDIN]-(messageY : POST |"
                                    + " COMMENT)-[:HASCREATOR]->(otherP:PERSON),\n"
                                    + "    (otherP:PERSON)-[:ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]->(countryCity:PLACE),\n"
                                    + "    (p:PERSON {id:"
                                    + " $personId})-[:KNOWS*1..3]-(otherP:PERSON)\n"
                                    + "WHERE \n"
                                    + "    otherP <> p\n"
                                    + "    AND messageX.creationDate >= $startDate\n"
                                    + "    AND messageX.creationDate < $endDate \n"
                                    + "    AND messageY.creationDate >= $startDate\n"
                                    + "    AND messageY.creationDate < $endDate \n"
                                    + "    AND countryCity.name <> $countryXName \n"
                                    + "    AND countryCity.name <> $countryYName\n"
                                    + "WITH \n"
                                    + "    otherP, \n"
                                    + "    count(messageX) as xCount, \n"
                                    + "    count(messageY) as yCount \n"
                                    + "RETURN\n"
                                    + "    otherP.id as id,\n"
                                    + "    otherP.firstName as firstName,\n"
                                    + "    otherP.lastName as lastName,\n"
                                    + "    xCount,\n"
                                    + "    yCount,\n"
                                    + "    xCount + yCount as total \n"
                                    + "ORDER BY total DESC, id ASC LIMIT 20;",
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
                    + "      LogicalFilter(condition=[<>(otherP, p)])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PLACE]}],"
                    + " alias=[countryY], fusedFilter=[[=(_.name, ?1)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, COMMENT,"
                    + " PLACE), EdgeLabel(ISLOCATEDIN, POST, PLACE)]], alias=[_],"
                    + " startAlias=[messageY], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[messageY], fusedFilter=[[AND(>=(_.creationDate, ?3),"
                    + " <(_.creationDate, ?4))]], opt=[START], physicalOpt=[ITSELF])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[_], startAlias=[otherP], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[countryX], fusedFilter=[[=(_.name, ?0)]],"
                    + " opt=[END], physicalOpt=[ITSELF])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " COMMENT, PLACE), EdgeLabel(ISLOCATEDIN, POST, PLACE)]], alias=[_],"
                    + " startAlias=[messageX], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "                    GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[POST, COMMENT]}], alias=[messageX],"
                    + " fusedFilter=[[AND(>=(_.creationDate, ?3), <(_.creationDate, ?4))]],"
                    + " opt=[START], physicalOpt=[ITSELF])\n"
                    + "                      GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[_], startAlias=[otherP], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                        GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[countryCity], fusedFilter=[[AND(<>(_.name, ?0),"
                    + " <>(_.name, ?1))]], opt=[END], physicalOpt=[ITSELF])\n"
                    + "                          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISPARTOF]}], alias=[_], startAlias=[city], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                           "
                    + " GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, PLACE)]],"
                    + " alias=[city], startAlias=[otherP], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "                              GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[otherP], opt=[END])\n"
                    + "                               "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[p])\n"
                    + "                                 "
                    + " GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}], alias=[p],"
                    + " opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?2)])",
                after.explain().trim());
    }

    @Test
    public void ldbc4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " $personId})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:"
                                    + " TAG)\n"
                                    + "WITH DISTINCT tag, post\n"
                                    + "WITH tag,\n"
                                    + "     CASE\n"
                                    + "       WHEN post.creationDate < $endDate  AND"
                                    + " post.creationDate >= $startDate THEN 1\n"
                                    + "       ELSE 0\n"
                                    + "     END AS valid,\n"
                                    + "     CASE\n"
                                    + "       WHEN $startDate > post.creationDate THEN 1\n"
                                    + "       ELSE 0\n"
                                    + "     END AS inValid\n"
                                    + "WITH tag, sum(valid) AS postCount, sum(inValid) AS"
                                    + " inValidPostCount\n"
                                    + "WHERE postCount>0 AND inValidPostCount=0\n"
                                    + "\n"
                                    + "RETURN tag.name AS tagName, postCount\n"
                                    + "ORDER BY postCount DESC, tagName ASC\n"
                                    + "LIMIT 10;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[postCount], sort1=[tagName], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[10])\n"
                    + "  GraphLogicalProject(tagName=[tag.name], postCount=[postCount],"
                    + " isAppend=[false])\n"
                    + "    LogicalFilter(condition=[AND(>(postCount, 0), =(inValidPostCount,"
                    + " 0))])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[tag], aliases=[tag]}],"
                    + " values=[[{operands=[valid], aggFunction=SUM, alias='postCount',"
                    + " distinct=false}, {operands=[inValid], aggFunction=SUM,"
                    + " alias='inValidPostCount', distinct=false}]])\n"
                    + "        GraphLogicalProject(tag=[tag], valid=[CASE(AND(<(post.creationDate,"
                    + " ?1), >=(post.creationDate, ?2)), 1, 0)], inValid=[CASE(>(?2,"
                    + " post.creationDate), 1, 0)], isAppend=[false])\n"
                    + "          GraphLogicalAggregate(keys=[{variables=[tag, post], aliases=[tag,"
                    + " post]}], values=[[]])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST,"
                    + " TAG)]], alias=[tag], startAlias=[post], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR,"
                    + " POST, PERSON)]], alias=[_], startAlias=[friend], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[friend], startAlias=[person], opt=[BOTH],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                    GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?0)])",
                after.explain().trim());
    }

    @Test
    public void ldbc5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(friend)\n"
                                        + "WITH DISTINCT friend\n"
                                        + "WHERE friend.id <> $personId\n"
                                        + "MATCH (friend)<-[membership:HASMEMBER]-(forum)\n"
                                        + "WHERE membership.joinDate > $minDate\n"
                                        + "CALL {\n"
                                        + "  WITH forum\n"
                                        + "  RETURN forum, 0 AS postCount\n"
                                        + "  ORDER BY forum.id ASC\n"
                                        + "  LIMIT 20\n"
                                        + "}\n"
                                        + "UNION\n"
                                        + "CALL {\n"
                                        + "  WITH friend, collect(forum) AS forums\n"
                                        + "  MATCH"
                                        + " (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
                                        + "  WHERE forum IN forums\n"
                                        + "  WITH forum, count(post) AS postCount\n"
                                        + "  RETURN forum, postCount\n"
                                        + "  ORDER BY postCount DESC, forum.id ASC\n"
                                        + "  LIMIT 20\n"
                                        + "}\n"
                                        + "WITH forum, max(postCount) AS postCount\n"
                                        + "RETURN forum, postCount\n"
                                        + "ORDER BY postCount DESC, forum.id ASC\n"
                                        + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                        + "GraphLogicalSort(sort0=[postCount], sort1=[forum.id], dir0=[DESC],"
                        + " dir1=[ASC], fetch=[20])\n"
                        + "  GraphLogicalProject(forum=[forum], postCount=[postCount],"
                        + " isAppend=[false])\n"
                        + "    GraphLogicalAggregate(keys=[{variables=[forum], aliases=[forum]}],"
                        + " values=[[{operands=[postCount], aggFunction=MAX, alias='postCount',"
                        + " distinct=false}]])\n"
                        + "      LogicalUnion(all=[true])\n"
                        + "        GraphLogicalSort(sort0=[forum.id], dir0=[ASC], fetch=[20])\n"
                        + "          GraphLogicalProject(forum=[forum], postCount=[0],"
                        + " isAppend=[false])\n"
                        + "            GraphLogicalProject(forum=[forum], isAppend=[false])\n"
                        + "              CommonTableScan(table=[[common#1874145243]])\n"
                        + "        GraphLogicalSort(sort0=[postCount], sort1=[forum.id], dir0=[DESC],"
                        + " dir1=[ASC], fetch=[20])\n"
                        + "          GraphLogicalProject(forum=[forum], postCount=[postCount],"
                        + " isAppend=[false])\n"
                        + "            GraphLogicalAggregate(keys=[{variables=[forum],"
                        + " aliases=[forum]}], values=[[{operands=[post], aggFunction=COUNT,"
                        + " alias='postCount', distinct=false}]])\n"
                        + "              LogicalFilter(condition=[IN(forum, forums)])\n"
                        + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[CONTAINEROF]}], alias=[forum], opt=[IN], physicalOpt=[VERTEX])\n"
                        + "                  GraphPhysicalGetV(tableConfig=[{isAll=false,"
                        + " tables=[POST]}], alias=[post], opt=[START], physicalOpt=[ITSELF])\n"
                        + "                    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR,"
                        + " POST, PERSON)]], alias=[_], startAlias=[friend], opt=[IN],"
                        + " physicalOpt=[VERTEX])\n"
                        + "                      GraphLogicalAggregate(keys=[{variables=[friend],"
                        + " aliases=[friend]}], values=[[{operands=[forum], aggFunction=COLLECT,"
                        + " alias='forums', distinct=false}]])\n"
                        + "                        CommonTableScan(table=[[common#1874145243]])\n"
                        + "common#1874145243:\n"
                        + "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[FORUM]}], alias=[forum],"
                        + " opt=[START])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                        + " alias=[membership], startAlias=[friend], fusedFilter=[[>(_.joinDate, ?1)]],"
                        + " opt=[IN])\n"
                        + "    LogicalFilter(condition=[<>(friend.id, ?0)])\n"
                        + "      GraphLogicalAggregate(keys=[{variables=[friend], aliases=[friend]}],"
                        + " values=[[]])\n"
                        + "        GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                        + " alias=[friend], opt=[END])\n"
                        + "         "
                        + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                        + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                        + " alias=[_], start_alias=[person])\n"
                        + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                        + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])",
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
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST, TAG)]],"
                    + " alias=[otherTag], startAlias=[post], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, _UTF-8'North_German_Confederation')]],"
                    + " opt=[END], physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST, TAG)]],"
                    + " alias=[_], startAlias=[post], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], opt=[START], physicalOpt=[ITSELF])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON)]], alias=[_], startAlias=[other], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[other], opt=[END])\n"
                    + "                 "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[person])\n"
                    + "                    GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " 2199023382370)])",
                after.explain().trim());
    }

    @Test
    public void ldbc6_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p_:PERSON {id: $personId})-[:KNOWS*1..3]-(other:PERSON)\n"
                                        + "WITH distinct other\n"
                                        + "WHERE other.id <> $personId\n"
                                        + "\n"
                                        + "MATCH (other)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG"
                                        + " {name: $tagName})\n"
                                        + "\n"
                                        + "Match (p:POST)-[:HASTAG]->(otherTag:TAG)\n"
                                        + "WHERE \n"
                                        + "    otherTag <> t \n"
                                        + "RETURN\n"
                                        + "    otherTag.name as name,\n"
                                        + "    count(distinct p) as postCnt \n"
                                        + "ORDER BY \n"
                                        + "    postCnt desc, \n"
                                        + "    name asc \n"
                                        + "LIMIT 10;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        //        System.out.println(after.explain());
    }

    // todo: fix issues in ldbc7: expand (with alias) + getV cannot be fused thus causing the
    // execution errors of extend intersect
    @Test
    public void ldbc7_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id: $personId})<-[:HASCREATOR]-(message:"
                                        + " POST | COMMENT)<-[like:LIKES]-(liker:PERSON)\n"
                                        + "OPTIONAL MATCH (liker: PERSON)-[k:KNOWS]-(person: PERSON"
                                        + " {id: $personId})\n"
                                        + "WITH liker, message, like.creationDate AS likeTime,"
                                        + " person,\n"
                                        + "  CASE\n"
                                        + "      WHEN k is null THEN true\n"
                                        + "      ELSE false\n"
                                        + "     END AS isNew\n"
                                        + "ORDER BY likeTime DESC, message.id ASC\n"
                                        + "WITH liker, person, head(collect(message)) as message,"
                                        + " head(collect(likeTime)) AS likeTime, isNew\n"
                                        + "RETURN\n"
                                        + "    liker.id AS personId,\n"
                                        + "    liker.firstName AS personFirstName,\n"
                                        + "    liker.lastName AS personLastName,\n"
                                        + "    likeTime AS likeCreationDate,\n"
                                        + "    message.id AS commentOrPostId,\n"
                                        + "    message.content AS messageContent,\n"
                                        + "    message.imageFile AS messageImageFile,\n"
                                        + "    (likeTime - message.creationDate)/1000/60 AS"
                                        + " minutesLatency,\n"
                                        + "  \tisNew\n"
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
                    + " messageContent=[message.content], messageImageFile=[message.imageFile],"
                    + " minutesLatency=[/(/(-(likeTime, message.creationDate), 1000), 60)],"
                    + " isNew=[isNew], isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[liker, person, isNew],"
                    + " aliases=[liker, person, isNew]}], values=[[{operands=[message],"
                    + " aggFunction=FIRST_VALUE, alias='message', distinct=false},"
                    + " {operands=[likeTime], aggFunction=FIRST_VALUE, alias='likeTime',"
                    + " distinct=false}]])\n"
                    + "      GraphLogicalSort(sort0=[likeTime], sort1=[message.id], dir0=[DESC],"
                    + " dir1=[ASC])\n"
                    + "        GraphLogicalProject(liker=[liker], message=[message],"
                    + " likeTime=[like.creationDate], person=[person], isNew=[IS NULL(k)],"
                    + " isAppend=[false])\n"
                    + "          MultiJoin(joinFilter=[=(liker, liker)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "            GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[liker], opt=[START])\n"
                    + "              GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[LIKES]}], alias=[like], startAlias=[message], opt=[IN])\n"
                    + "                CommonTableScan(table=[[common#378747223]])\n"
                    + "            GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[liker], opt=[OTHER])\n"
                    + "              GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[k], startAlias=[person], opt=[BOTH],"
                    + " optional=[true])\n"
                    + "                CommonTableScan(table=[[common#378747223]])\n"
                    + "common#378747223:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[message], startAlias=[person], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])",
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
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[author], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
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
                                "MATCH (p:PERSON {id: $personId})-[:KNOWS*1..3]-(friend:PERSON)\n"
                                        + "WITH distinct friend\n"
                                        + "where friend.id <> $personId\n"
                                        + "MATCH "
                                        + " (message:POST|COMMENT)-[:HASCREATOR]->(friend:PERSON)\n"
                                        + "where message.creationDate < $maxDate\n"
                                        + "WITH friend, message\n"
                                        + "\n"
                                        + "RETURN \n"
                                        + "    friend.id AS personId, \n"
                                        + "    friend.firstName AS personFirstName, \n"
                                        + "    friend.lastName AS personLastName, \n"
                                        + "    message.id AS commentOrPostId, \n"
                                        + "    message.content AS messageContent, \n"
                                        + "    message.imageFile AS messageImageFile, \n"
                                        + "    message.creationDate AS commentOrPostCreationDate\n"
                                        + "ORDER BY \n"
                                        + "    commentOrPostCreationDate DESC, \n"
                                        + "    commentOrPostId ASC \n"
                                        + "LIMIT 20",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[commentOrPostCreationDate], sort1=[commentOrPostId],"
                    + " dir0=[DESC], dir1=[ASC], fetch=[20])\n"
                    + "  GraphLogicalProject(personId=[friend.id],"
                    + " personFirstName=[friend.firstName], personLastName=[friend.lastName],"
                    + " commentOrPostId=[message.id], messageContent=[message.content],"
                    + " messageImageFile=[message.imageFile],"
                    + " commentOrPostCreationDate=[message.creationDate], isAppend=[false])\n"
                    + "    GraphLogicalProject(friend=[friend], message=[message],"
                    + " isAppend=[false])\n"
                    + "      GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], fusedFilter=[[<(_.creationDate, ?1)]], opt=[START],"
                    + " physicalOpt=[ITSELF])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON), EdgeLabel(HASCREATOR, COMMENT, PERSON)]], alias=[_],"
                    + " startAlias=[friend], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "          LogicalFilter(condition=[<>(friend.id, ?0)])\n"
                    + "            GraphLogicalAggregate(keys=[{variables=[friend],"
                    + " aliases=[friend]}], values=[[]])\n"
                    + "              GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[friend], opt=[END])\n"
                    + "               "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[p])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[p], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?0)])",
                after.explain().trim());
    }

    @Test
    public void ldbc10_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id: $personId})-[:KNOWS*2..3]-(friend:"
                                        + " PERSON)-[:ISLOCATEDIN]->(city:PLACE)\n"
                                        + "WHERE \n"
                                        + "        NOT friend=person \n"
                                        + "        AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON"
                                        + " {id: $personId})\n"
                                        + "WITH \n"
                                        + "        person, \n"
                                        + "        city, \n"
                                        + "        friend, \n"
                                        + "        friend.birthday as birthday\n"
                                        + "WITH DISTINCT friend, city, person\n"
                                        + "\n"
                                        + "OPTIONAL MATCH (friend :"
                                        + " PERSON)<-[:HASCREATOR]-(post:POST)\n"
                                        + "WITH friend, city, person, count(post) as postCount\n"
                                        + "\n"
                                        + "OPTIONAL MATCH"
                                        + " (friend)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(person:"
                                        + " PERSON {id: $personId})\n"
                                        + "WITH friend, city, postCount, count(distinct post1) as"
                                        + " commonPostCount\n"
                                        + "\n"
                                        + "RETURN friend.id AS personId,\n"
                                        + "       friend.firstName AS personFirstName,\n"
                                        + "       friend.lastName AS personLastName,\n"
                                        + "       commonPostCount - (postCount - commonPostCount) AS"
                                        + " commonInterestScore,\n"
                                        + "       friend.gender AS personGender,\n"
                                        + "       city.name AS personCityName\n"
                                        + "ORDER BY commonInterestScore DESC, personId ASC\n"
                                        + "LIMIT 10;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[commonInterestScore], sort1=[personId], dir0=[DESC],"
                        + " dir1=[ASC], fetch=[10])\n"
                        + "  GraphLogicalProject(personId=[friend.id],"
                        + " personFirstName=[friend.firstName], personLastName=[friend.lastName],"
                        + " commonInterestScore=[-(commonPostCount, -(postCount, commonPostCount))],"
                        + " personGender=[friend.gender], personCityName=[city.name],"
                        + " isAppend=[false])\n"
                        + "    GraphLogicalAggregate(keys=[{variables=[friend, city, postCount],"
                        + " aliases=[friend, city, postCount]}], values=[[{operands=[post1],"
                        + " aggFunction=COUNT, alias='commonPostCount', distinct=true}]])\n"
                        + "      LogicalJoin(condition=[AND(=(friend, friend), =(person, person))],"
                        + " joinType=[left])\n"
                        + "        GraphLogicalAggregate(keys=[{variables=[friend, city, person],"
                        + " aliases=[friend, city, person]}], values=[[{operands=[post],"
                        + " aggFunction=COUNT, alias='postCount', distinct=false}]])\n"
                        + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                        + " alias=[post], opt=[START], physicalOpt=[ITSELF])\n"
                        + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                        + " PERSON)]], alias=[_], startAlias=[friend], opt=[IN], physicalOpt=[VERTEX],"
                        + " optional=[true])\n"
                        + "              GraphLogicalAggregate(keys=[{variables=[friend, city, person],"
                        + " aliases=[friend, city, person]}], values=[[]])\n"
                        + "                GraphLogicalProject(person=[person], city=[city],"
                        + " friend=[friend], birthday=[friend.birthday], isAppend=[false])\n"
                        + "                  LogicalJoin(condition=[AND(=(person, person), =(friend,"
                        + " friend))], joinType=[anti])\n"
                        + "                    LogicalFilter(condition=[<>(friend, person)])\n"
                        + "                     "
                        + " GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, PLACE)]],"
                        + " alias=[city], startAlias=[friend], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "                        GraphLogicalGetV(tableConfig=[{isAll=false,"
                        + " tables=[PERSON]}], alias=[friend], opt=[END])\n"
                        + "                         "
                        + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                        + "], offset=[2], fetch=[1], path_opt=[ARBITRARY], result_opt=[END_V],"
                        + " alias=[_], start_alias=[person])\n"
                        + "                            GraphLogicalSource(tableConfig=[{isAll=false,"
                        + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                        + " ?0)])\n"
                        + "                    GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[KNOWS]}], alias=[friend], startAlias=[person], opt=[BOTH],"
                        + " physicalOpt=[VERTEX])\n"
                        + "                      GraphLogicalSource(tableConfig=[{isAll=false,"
                        + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                        + " ?0)])\n"
                        + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                        + " PERSON)]], alias=[friend], startAlias=[post1], opt=[OUT],"
                        + " physicalOpt=[VERTEX])\n"
                        + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                        + " alias=[post1], opt=[START], physicalOpt=[ITSELF])\n"
                        + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST,"
                        + " TAG)]], alias=[_], startAlias=[tag], opt=[IN], physicalOpt=[VERTEX])\n"
                        + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[HASINTEREST]}], alias=[tag], startAlias=[person], opt=[OUT],"
                        + " physicalOpt=[VERTEX])\n"
                        + "                GraphLogicalSource(tableConfig=[{isAll=false,"
                        + " tables=[PERSON]}], alias=[person], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                        + " ?0)])",
                after.explain().trim());
    }

    @Test
    public void ldbc11_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:PERSON {id:"
                                    + " $personId})-[:KNOWS*1..3]-(friend:PERSON)-[wa:WORKAT]->(com:ORGANISATION)-[:ISLOCATEDIN]->(:PLACE"
                                    + " {name: $countryName}) \n"
                                    + "WHERE \n"
                                    + "    p <> friend \n"
                                    + "    AND wa.workFrom < $workFromYear\n"
                                    + "WITH DISTINCT \n"
                                    + "    friend as friend, \n"
                                    + "    com AS com, \n"
                                    + "    wa.workFrom as organizationWorkFromYear \n"
                                    + "ORDER BY \n"
                                    + "    organizationWorkFromYear ASC, \n"
                                    + "    friend.id ASC, com.name DESC \n"
                                    + "LIMIT 10 \n"
                                    + "return \n"
                                    + "    friend.id AS personId, \n"
                                    + "    friend.firstName AS personFirstName, \n"
                                    + "    friend.lastName AS personLastName, \n"
                                    + "    com.name as organizationName, \n"
                                    + "    organizationWorkFromYear as organizationWorkFromYear;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(personId=[friend.id], personFirstName=[friend.firstName],"
                    + " personLastName=[friend.lastName], organizationName=[com.name],"
                    + " organizationWorkFromYear=[organizationWorkFromYear], isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[organizationWorkFromYear], sort1=[friend.id],"
                    + " sort2=[com.name], dir0=[ASC], dir1=[ASC], dir2=[DESC], fetch=[10])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[friend, com, wa.workFrom],"
                    + " aliases=[friend, com, organizationWorkFromYear]}], values=[[]])\n"
                    + "      LogicalFilter(condition=[<>(p, friend)])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PLACE]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, ?1)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " ORGANISATION, PLACE)]], alias=[_], startAlias=[com], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[ORGANISATION]}], alias=[com], opt=[END])\n"
                    + "              GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[WORKAT]}], alias=[wa], startAlias=[friend],"
                    + " fusedFilter=[[<(_.workFrom, ?2)]], opt=[OUT])\n"
                    + "                GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[friend], opt=[END])\n"
                    + "                 "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[p])\n"
                    + "                    GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[p], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?0)])",
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
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST, TAG)]],"
                    + " alias=[tag], startAlias=[PATTERN_VERTEX$5], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[PATTERN_VERTEX$5], opt=[END], physicalOpt=[ITSELF])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT,"
                    + " POST)]], alias=[_], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[COMMENT]}], alias=[comment], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR,"
                    + " COMMENT, PERSON)]], alias=[_], startAlias=[friend], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                    GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[friend], startAlias=[PATTERN_VERTEX$0],"
                    + " opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "                      GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[PATTERN_VERTEX$0], opt=[VERTEX],"
                    + " uniqueKeyFilters=[=(_.id, 2199023382370)])",
                after.explain().trim());
    }

    @Test
    public void ldbc14_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH all ShortestPath((person1:PERSON { id: $person1Id"
                                    + " })-[path:KNOWS*0..10]-(person2:PERSON { id: $person2Id"
                                    + " }))\n"
                                    + "WITH path, gs.function.relationships(path) as rels_in_path,"
                                    + " gs.function.nodes(path) as nodes_in_path\n"
                                    + "UNWIND rels_in_path as rel\n"
                                    + "WITH path, rels_in_path, nodes_in_path,"
                                    + " gs.function.startNode(rel) as rel0,"
                                    + " gs.function.endNode(rel) as rel1\n"
                                    + "OPTIONAL MATCH"
                                    + " (rel0:PERSON)<-[:HASCREATOR]-(n)-[:REPLYOF]-(m)-[:HASCREATOR]->(rel1:PERSON)\n"
                                    + "With path, nodes_in_path, rels_in_path,\n"
                                    + "    CASE WHEN labels(m) <> labels(n) THEN 1 ELSE 0 END as"
                                    + " ra,\n"
                                    + "    CASE WHEN labels(m)  = labels(n) THEN 1 ELSE 0 END as"
                                    + " rb\n"
                                    + "With path, nodes_in_path, rels_in_path, SUM(ra) AS"
                                    + " weight1Count, SUM(rb) as weight2Count\n"
                                    + "UNWIND nodes_in_path as node\n"
                                    + "WITH path, COLLECT(node.id) as personIdsInPath,"
                                    + " weight1Count, weight2Count\n"
                                    + "RETURN personIdsInPath, (weight1Count +"
                                    + " gs.function.toFloat(weight2Count) / 2) AS pathWeight\n"
                                    + "ORDER BY pathWeight DESC;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[pathWeight], dir0=[DESC])\n"
                    + "  GraphLogicalProject(personIdsInPath=[personIdsInPath],"
                    + " pathWeight=[+(weight1Count, /(gs.function.toFloat(weight2Count), 2))],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[path, weight1Count,"
                    + " weight2Count], aliases=[path, weight1Count, weight2Count]}],"
                    + " values=[[{operands=[node.id], aggFunction=COLLECT, alias='personIdsInPath',"
                    + " distinct=false}]])\n"
                    + "      GraphLogicalUnfold(key=[nodes_in_path], alias=[node])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[path, nodes_in_path,"
                    + " rels_in_path], aliases=[path, nodes_in_path, rels_in_path]}],"
                    + " values=[[{operands=[ra], aggFunction=SUM, alias='weight1Count',"
                    + " distinct=false}, {operands=[rb], aggFunction=SUM, alias='weight2Count',"
                    + " distinct=false}]])\n"
                    + "          GraphLogicalProject(path=[path], nodes_in_path=[nodes_in_path],"
                    + " rels_in_path=[rels_in_path], ra=[CASE(<>(m.~label, n.~label), 1, 0)],"
                    + " rb=[CASE(=(m.~label, n.~label), 1, 0)], isAppend=[false])\n"
                    + "            MultiJoin(joinFilter=[=(m, m)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT,"
                    + " POST), EdgeLabel(REPLYOF, COMMENT, COMMENT)]], alias=[m], opt=[BOTH],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[n], startAlias=[rel0], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "                  CommonTableScan(table=[[common#-1230129050]])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON), EdgeLabel(HASCREATOR, COMMENT, PERSON)]], alias=[m],"
                    + " startAlias=[rel1], opt=[IN], physicalOpt=[VERTEX], optional=[true])\n"
                    + "                CommonTableScan(table=[[common#-1230129050]])\n"
                    + "common#-1230129050:\n"
                    + "GraphLogicalProject(path=[path], rels_in_path=[rels_in_path],"
                    + " nodes_in_path=[nodes_in_path], rel0=[gs.function.startNode(rel)],"
                    + " rel1=[gs.function.endNode(rel)], isAppend=[false])\n"
                    + "  GraphLogicalUnfold(key=[rels_in_path], alias=[rel])\n"
                    + "    GraphLogicalProject(path=[path],"
                    + " rels_in_path=[gs.function.relationships(path)],"
                    + " nodes_in_path=[gs.function.nodes(path)], isAppend=[false])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], fusedFilter=[[=(_.id, ?1)]], opt=[END])\n"
                    + "       "
                    + " GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[_], opt=[OTHER])\n"
                    + "], fetch=[10], path_opt=[ALL_SHORTEST], result_opt=[ALL_V_E], alias=[path],"
                    + " start_alias=[person1])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }
}
