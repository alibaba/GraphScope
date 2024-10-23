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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BITest {
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
                                "FilterIntoJoinRule, FilterMatchRule,"
                                        + " ExtendIntersectRule, ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc_schema_exp_hierarchy.json",
                        "statistics/ldbc30_hierarchy_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @AfterClass
    public static void afterClass() {
        if (optimizer != null) {
            optimizer.close();
        }
    }

    @Test
    public void bi1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (message:POST|COMMENT)\n"
                                    + "WHERE message.creationDate < $datetime\n"
                                    + "WITH count(message) AS totalMessageCount\n"
                                    + "\n"
                                    + "MATCH (message:POST|COMMENT)\n"
                                    + "WHERE message.creationDate < $datetime\n"
                                    + "  \t\tAND message.content IS NOT NULL\n"
                                    + "WITH\n"
                                    + "  totalMessageCount,\n"
                                    + "  message,\n"
                                    + "  message.creationDate as date\n"
                                    + "WITH\n"
                                    + "  totalMessageCount,\n"
                                    + "  message,\n"
                                    + "  date.year AS year\n"
                                    + "WITH\n"
                                    + "  totalMessageCount,\n"
                                    + "  year,\n"
                                    + "  labels(message) = 'Comment' AS isComment,\n"
                                    + "  CASE\n"
                                    + "    WHEN message.length <  40 THEN 0\n"
                                    + "    WHEN message.length <  80 THEN 1\n"
                                    + "    WHEN message.length < 160 THEN 2\n"
                                    + "    ELSE                           3\n"
                                    + "  END AS lengthCategory,\n"
                                    + "  count(message) AS messageCount,\n"
                                    + "  sum(message.length) / count(message) AS"
                                    + " averageMessageLength,\n"
                                    + "  sum(message.length) AS sumMessageLength\n"
                                    + "RETURN\n"
                                    + "  year,\n"
                                    + "  isComment,\n"
                                    + "  lengthCategory,\n"
                                    + "  messageCount,\n"
                                    + "  averageMessageLength,\n"
                                    + "  sumMessageLength,\n"
                                    + "  messageCount / totalMessageCount AS percentageOfMessages\n"
                                    + "ORDER BY\n"
                                    + "  year DESC,\n"
                                    + "  isComment ASC,\n"
                                    + "  lengthCategory ASC;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[year], sort1=[isComment], sort2=[lengthCategory],"
                    + " dir0=[DESC], dir1=[ASC], dir2=[ASC])\n"
                    + "  GraphLogicalProject(year=[year], isComment=[isComment],"
                    + " lengthCategory=[lengthCategory], messageCount=[messageCount],"
                    + " averageMessageLength=[averageMessageLength],"
                    + " sumMessageLength=[sumMessageLength], percentageOfMessages=[/(messageCount,"
                    + " totalMessageCount)], isAppend=[false])\n"
                    + "    GraphLogicalProject(totalMessageCount=[totalMessageCount], year=[year],"
                    + " isComment=[isComment], lengthCategory=[lengthCategory],"
                    + " messageCount=[messageCount], sumMessageLength=[sumMessageLength],"
                    + " averageMessageLength=[/(EXPR$2, EXPR$3)], isAppend=[false])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[totalMessageCount, year, $f0,"
                    + " $f1], aliases=[totalMessageCount, year, isComment, lengthCategory]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='messageCount',"
                    + " distinct=false}, {operands=[message.length], aggFunction=SUM,"
                    + " alias='EXPR$2', distinct=false}, {operands=[message], aggFunction=COUNT,"
                    + " alias='EXPR$3', distinct=false}, {operands=[message.length],"
                    + " aggFunction=SUM, alias='sumMessageLength', distinct=false}]])\n"
                    + "        GraphLogicalProject($f0=[=(message.~label, _UTF-8'Comment')],"
                    + " $f1=[CASE(<(message.length, 40), 0, <(message.length, 80), 1,"
                    + " <(message.length, 160), 2, 3)], isAppend=[true])\n"
                    + "          GraphLogicalProject(totalMessageCount=[totalMessageCount],"
                    + " message=[message], year=[EXTRACT(FLAG(YEAR), date)], isAppend=[false])\n"
                    + "            GraphLogicalProject(totalMessageCount=[totalMessageCount],"
                    + " message=[message], date=[message.creationDate], isAppend=[false])\n"
                    + "              LogicalJoin(condition=[true], joinType=[inner])\n"
                    + "                GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='totalMessageCount',"
                    + " distinct=false}]])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[POST, COMMENT]}], alias=[message], fusedFilter=[[<(_.creationDate,"
                    + " ?0)]], opt=[VERTEX])\n"
                    + "                GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], fusedFilter=[[<(_.creationDate, ?0)]],"
                    + " opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (country:COUNTRY {name:"
                                    + " \"China\"})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-\n"
                                    + "  (person:PERSON)<-[:HASMODERATOR]-(forum:FORUM)-[:CONTAINEROF]->\n"
                                    + "  (post:POST)<-[:REPLYOF*0..6]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS"
                                    + " {name: \"Song\"})\n"
                                    + "RETURN\n"
                                    + "\tforum.id as id,\n"
                                    + "  forum.title,\n"
                                    + "  forum.creationDate,\n"
                                    + "\tcount(DISTINCT message) AS messageCount\n"
                                    + "ORDER BY\n"
                                    + "  messageCount DESC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[messageCount], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[forum.id, forum.title,"
                    + " forum.creationDate], aliases=[id, title, creationDate]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='messageCount',"
                    + " distinct=true}]])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[TAGCLASS]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, _UTF-8'Song')]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTYPE]}],"
                    + " alias=[_], startAlias=[PATTERN_VERTEX$11], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[PATTERN_VERTEX$11],"
                    + " startAlias=[message], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[END])\n"
                    + "           "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[6], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[post])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[post], startAlias=[forum], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMODERATOR]}], alias=[forum], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                  GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                    GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " PERSON, CITY)]], alias=[_], startAlias=[PATTERN_VERTEX$1], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                      GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISPARTOF,"
                    + " CITY, COUNTRY)]], alias=[PATTERN_VERTEX$1], startAlias=[country], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                        GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[COUNTRY]}], alias=[country], fusedFilter=[[=(_.name,"
                    + " _UTF-8'China')]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (forum:FORUM)\n"
                                    + "WITH collect(forum) AS topForums\n"
                                    + "UNWIND topForums AS topForums1\n"
                                    + "MATCH"
                                    + " (topForums1:FORUM)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..10]-(message:POST|COMMENT)-[:HASCREATOR]->(person:PERSON)<-[:HASMEMBER]-(topForums2:FORUM)\n"
                                    + "WHERE topForums2 IN topForums\n"
                                    + "RETURN\n"
                                    + "  person.id AS personId\n"
                                    + "  ORDER BY\n"
                                    + "  personId ASC\n"
                                    + "  LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[personId], dir0=[ASC], fetch=[100])\n"
                    + "  GraphLogicalProject(personId=[person.id], isAppend=[false])\n"
                    + "    LogicalJoin(condition=[AND(=(topForums1, topForums1), IN(topForums2,"
                    + " topForums))], joinType=[inner])\n"
                    + "      GraphLogicalUnfold(key=[topForums], alias=[topForums1])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[forum], aggFunction=COLLECT, alias='topForums',"
                    + " distinct=false}]])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[topForums2], startAlias=[person], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[person], startAlias=[message], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[END])\n"
                    + "           "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[10], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[post])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[post], startAlias=[topForums1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[FORUM]}], alias=[topForums1], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (tag:TAG {name:"
                                    + " $tag})<-[:HASTAG]-(message:POST|COMMENT)-[:HASCREATOR]->(person:PERSON)\n"
                                    + "OPTIONAL MATCH (message)<-[likes:LIKES]-(:PERSON)\n"
                                    + "OPTIONAL MATCH (message)<-[:REPLYOF]-(reply:COMMENT)\n"
                                    + "WITH \n"
                                    + "\tperson, \n"
                                    + "  count(likes) AS likeCount,\n"
                                    + "  count(reply) AS replyCount,\n"
                                    + "  count(message) AS messageCount\n"
                                    + "RETURN\n"
                                    + "  person.id AS id,\n"
                                    + "  replyCount,\n"
                                    + "  likeCount,\n"
                                    + "  messageCount,\n"
                                    + "  1*messageCount + 2*replyCount + 10*likeCount AS score\n"
                                    + "ORDER BY\n"
                                    + "  score DESC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[score], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalProject(id=[person.id], replyCount=[replyCount],"
                    + " likeCount=[likeCount], messageCount=[messageCount],"
                    + " score=[+(+(messageCount, *(2, replyCount)), *(10, likeCount))],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[likes], aggFunction=COUNT, alias='likeCount',"
                    + " distinct=false}, {operands=[reply], aggFunction=COUNT, alias='replyCount',"
                    + " distinct=false}, {operands=[message], aggFunction=COUNT,"
                    + " alias='messageCount', distinct=false}]])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[_], opt=[START])\n"
                    + "        GraphLogicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[likes], startAlias=[message], opt=[IN], optional=[true])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[REPLYOF]}],"
                    + " alias=[reply], startAlias=[message], opt=[IN], physicalOpt=[VERTEX],"
                    + " optional=[true])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[person], startAlias=[message], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[TAG]}], alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi6_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (tag:TAG {name:"
                                    + " $tag})<-[:HASTAG]-(message1:POST|COMMENT)-[:HASCREATOR]->(person1:PERSON)\n"
                                    + "OPTIONAL MATCH (message1)<-[:LIKES]-(person2:PERSON)\n"
                                    + "OPTIONAL MATCH"
                                    + " (person2)<-[:HASCREATOR]-(message2:POST|COMMENT)<-[like:LIKES]-(person3:PERSON)\n"
                                    + "RETURN\n"
                                    + "  person1.id as id,\n"
                                    + "  count(DISTINCT like) AS authorityScore\n"
                                    + "ORDER BY\n"
                                    + "  authorityScore DESC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[authorityScore], sort1=[id], dir0=[DESC],"
                    + " dir1=[ASC], fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person1.id], aliases=[id]}],"
                    + " values=[[{operands=[like], aggFunction=COUNT, alias='authorityScore',"
                    + " distinct=true}]])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person3], opt=[START])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[like], startAlias=[message2], opt=[IN], optional=[true])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[message2], startAlias=[person2], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[person2], startAlias=[message1], opt=[IN], physicalOpt=[VERTEX],"
                    + " optional=[true])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[person1], startAlias=[message1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message1], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[TAG]}], alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi7_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (tag:TAG {name:"
                                    + " $tag})<-[:HASTAG]-(message:POST|COMMENT),\n"
                                    + "  (message)<-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)\n"
                                    + "WHERE NOT (comment:COMMENT)-[:HASTAG]->(tag:TAG {name:"
                                    + " $tag})\n"
                                    + "RETURN\n"
                                    + "  relatedTag.name as name,\n"
                                    + "  count(DISTINCT comment) AS count\n"
                                    + "ORDER BY\n"
                                    + "  count DESC,\n"
                                    + "  name ASC\n"
                                    + "LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[count], sort1=[name], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[relatedTag.name],"
                    + " aliases=[name]}], values=[[{operands=[comment], aggFunction=COUNT,"
                    + " alias='count', distinct=true}]])\n"
                    + "    LogicalJoin(condition=[AND(=(tag, tag), =(comment, comment))],"
                    + " joinType=[anti])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[relatedTag], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[REPLYOF]}],"
                    + " alias=[comment], startAlias=[message], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])\n"
                    + "      GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[comment], opt=[START], physicalOpt=[ITSELF])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[_], startAlias=[tag], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi9_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "\t(person:PERSON)<-[:HASCREATOR]-(post:POST)<-[:REPLYOF*0..7]-(msg)\n"
                                    + "WHERE \n"
                                    + "\tpost.creationDate >= $date1 AND post.creationDate <="
                                    + " $date2\n"
                                    + "\tAND\n"
                                    + "\tmsg.creationDate >= $date1 AND msg.creationDate <="
                                    + " $date2\n"
                                    + "WITH \n"
                                    + "\tperson, \n"
                                    + "\tcount(distinct post) as threadCnt, \n"
                                    + "\tcount(distinct msg) as msgCnt\n"
                                    + "RETURN \n"
                                    + "\tperson.id as id,\n"
                                    + "\tthreadCnt,\n"
                                    + "\tmsgCnt\n"
                                    + "ORDER BY\n"
                                    + "\tmsgCnt DESC,\n"
                                    + "\tid ASC\n"
                                    + "LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[msgCnt], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalProject(id=[person.id], threadCnt=[threadCnt],"
                    + " msgCnt=[msgCnt], isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[post], aggFunction=COUNT, alias='threadCnt',"
                    + " distinct=true}, {operands=[msg], aggFunction=COUNT, alias='msgCnt',"
                    + " distinct=true}]])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[msg], fusedFilter=[[AND(>=(_.creationDate, ?0), <=(_.creationDate,"
                    + " ?1))]], opt=[END])\n"
                    + "       "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[7], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[post])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON)]], alias=[person], startAlias=[post], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], fusedFilter=[[AND(>=(_.creationDate, ?0), <=(_.creationDate,"
                    + " ?1))]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi10_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (p1:PERSON {id : $id})-[k:KNOWS*1..10]-(expert:PERSON)\n"
                                    + "MATCH"
                                    + " (expert)-[:ISLOCATEDIN]->(:CITY)-[:ISPARTOF]->(:COUNTRY"
                                    + " {name: $country}),\n"
                                    + "    \t(expert)<-[:HASCREATOR]-(message)-[:HASTAG]->(tag:TAG)-[:HASTYPE]->(:TAGCLASS"
                                    + " {name: $class})\n"
                                    + "WITH expert, tag, message, min(length(k)) as len\n"
                                    + "WHERE len >= $min\n"
                                    + "RETURN\n"
                                    + "  expert.id as id,\n"
                                    + "  tag.name as name,\n"
                                    + "  count(DISTINCT message) AS messageCount\n"
                                    + "ORDER BY\n"
                                    + "  messageCount DESC,\n"
                                    + "  name ASC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[messageCount], sort1=[name], sort2=[id],"
                    + " dir0=[DESC], dir1=[ASC], dir2=[ASC], fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[expert.id, tag.name], aliases=[id,"
                    + " name]}], values=[[{operands=[message], aggFunction=COUNT,"
                    + " alias='messageCount', distinct=true}]])\n"
                    + "    LogicalFilter(condition=[>=(len, ?3)])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[expert, tag, message],"
                    + " aliases=[expert, tag, message]}], values=[[{operands=[k.~len],"
                    + " aggFunction=MIN, alias='len', distinct=false}]])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[TAGCLASS]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, ?2)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTYPE]}],"
                    + " alias=[_], startAlias=[tag], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[tag], startAlias=[message],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[message], startAlias=[expert], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[COUNTRY]}], alias=[_], fusedFilter=[[=(_.name, ?1)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISPARTOF,"
                    + " CITY, COUNTRY)]], alias=[_], startAlias=[PATTERN_VERTEX$3], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                    GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " PERSON, CITY)]], alias=[PATTERN_VERTEX$3], startAlias=[expert], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                      GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[expert], opt=[END])\n"
                    + "                       "
                    + " GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[_], opt=[OTHER])\n"
                    + "], offset=[1], fetch=[9], path_opt=[ARBITRARY], result_opt=[ALL_V_E],"
                    + " alias=[k], start_alias=[p1])\n"
                    + "                          GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[p1], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?0)])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi11_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH \n"
                                    + "\t(s:COUNTRY {name:"
                                    + " \"India\"})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-(a),\n"
                                    + "\t(s:COUNTRY {name:"
                                    + " \"India\"})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-(b),\n"
                                    + "\t(s:COUNTRY {name:"
                                    + " \"India\"})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-(c),\n"
                                    + "\t(a)-[k1:KNOWS]-(b),\n"
                                    + "\t(a)-[k2:KNOWS]-(c),\n"
                                    + "\t(b)-[k3:KNOWS]-(c)\n"
                                    + "WHERE \n"
                                    + "\tk1.creationDate >= $date1 and k1.creationDate <= $date2\n"
                                    + "\tand k2.creationDate >= $date1 and k2.creationDate <="
                                    + " $date2\n"
                                    + "\tand k3.creationDate >= $date1 and k3.creationDate <="
                                    + " $date2\n"
                                    + "WITH distinct a, b, c\n"
                                    + "WHERE a.id < b.id and b.id < c.id\n"
                                    + "Return count(a);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[a], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  LogicalFilter(condition=[AND(<(a.id, b.id), <(b.id, c.id))])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[a, b, c], aliases=[a, b, c]}],"
                    + " values=[[]])\n"
                    + "      MultiJoin(joinFilter=[=(PATTERN_VERTEX$1, PATTERN_VERTEX$1)],"
                    + " isFullOuterJoin=[false], joinTypes=[[INNER, INNER]],"
                    + " outerJoinConditions=[[NULL, NULL]], projFields=[[ALL, ALL]])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " CITY)]], alias=[PATTERN_VERTEX$1], startAlias=[a], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          CommonTableScan(table=[[common#-1227130258]])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISPARTOF, CITY,"
                    + " COUNTRY)]], alias=[PATTERN_VERTEX$1], startAlias=[s], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          CommonTableScan(table=[[common#-1227130258]])\n"
                    + "common#-1227130258:\n"
                    + "MultiJoin(joinFilter=[=(a, a)], isFullOuterJoin=[false], joinTypes=[[INNER,"
                    + " INNER]], outerJoinConditions=[[NULL, NULL]], projFields=[[ALL, ALL]])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}], alias=[a],"
                    + " opt=[OTHER])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[k1], startAlias=[b], fusedFilter=[[AND(>=(_.creationDate, ?0),"
                    + " <=(_.creationDate, ?1))]], opt=[BOTH])\n"
                    + "      CommonTableScan(table=[[common#174543014]])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}], alias=[a],"
                    + " opt=[OTHER])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[k2], startAlias=[c], fusedFilter=[[AND(>=(_.creationDate, ?0),"
                    + " <=(_.creationDate, ?1))]], opt=[BOTH])\n"
                    + "      CommonTableScan(table=[[common#174543014]])\n"
                    + "common#174543014:\n"
                    + "MultiJoin(joinFilter=[=(PATTERN_VERTEX$9, PATTERN_VERTEX$9)],"
                    + " isFullOuterJoin=[false], joinTypes=[[INNER, INNER]],"
                    + " outerJoinConditions=[[NULL, NULL]], projFields=[[ALL, ALL]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, CITY)]],"
                    + " alias=[PATTERN_VERTEX$9], startAlias=[b], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-184198145]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISPARTOF, CITY, COUNTRY)]],"
                    + " alias=[PATTERN_VERTEX$9], startAlias=[s], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-184198145]])\n"
                    + "common#-184198145:\n"
                    + "GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}], alias=[b],"
                    + " opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[k3], startAlias=[c], fusedFilter=[[AND(>=(_.creationDate, ?0),"
                    + " <=(_.creationDate, ?1))]], opt=[BOTH])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[c], opt=[START], physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " CITY)]], alias=[_], startAlias=[PATTERN_VERTEX$5], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISPARTOF, CITY,"
                    + " COUNTRY)]], alias=[PATTERN_VERTEX$5], startAlias=[s], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[COUNTRY]}],"
                    + " alias=[s], fusedFilter=[[=(_.name, _UTF-8'India')]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi16_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person1:PERSON)<-[:HASCREATOR]-(msg)-[:HASTAG]->(tag:TAG)\n"
                                    + "OPTIONAL MATCH"
                                    + " (person1)-[:KNOWS]-(person2:PERSON)<-[:HASCREATOR]-(msg)\n"
                                    + "WHERE msg.creationDate = $date1 AND tag.name = $name1\n"
                                    + "    \tOR msg.creationDate = $date2 AND tag.name = $name2\n"
                                    + "WITH \n"
                                    + "\tperson1,\n"
                                    + "  person2,\n"
                                    + "  CASE WHEN msg.creationDate = $date1 AND tag.name = $name1"
                                    + " THEN 1\n"
                                    + "       ELSE 0\n"
                                    + "  END as msg1,\n"
                                    + "  CASE WHEN msg.creationDate = $date2 AND tag.name = $name2"
                                    + " THEN 1\n"
                                    + "       ELSE 0\n"
                                    + "  END as msg2\n"
                                    + "WITH\n"
                                    + "\tperson1,\n"
                                    + "  count(person2) as p2Cnt,\n"
                                    + "  count(DISTINCT msg1) as msg1Cnt,\n"
                                    + "  count(DISTINCT msg2) as msg2Cnt\n"
                                    + "WHERE p2Cnt <= $max_limit AND msg1Cnt > 0 AND msg2Cnt > 0\n"
                                    + "RETURN \n"
                                    + "\tperson1,\n"
                                    + "  msg1Cnt,\n"
                                    + "  msg2Cnt\n"
                                    + "ORDER BY\n"
                                    + "\tmsg1Cnt + msg2Cnt DESC,\n"
                                    + "  person1.id ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject(person1=[person1], msg1Cnt=[msg1Cnt], msg2Cnt=[msg2Cnt],"
                    + " isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[$f0], sort1=[person1.id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "    GraphLogicalProject($f0=[+(msg1Cnt, msg2Cnt)], isAppend=[true])\n"
                    + "      GraphLogicalProject(person1=[person1], msg1Cnt=[msg1Cnt],"
                    + " msg2Cnt=[msg2Cnt], isAppend=[false])\n"
                    + "        LogicalFilter(condition=[AND(<=(p2Cnt, ?4), >(msg1Cnt, 0),"
                    + " >(msg2Cnt, 0))])\n"
                    + "          GraphLogicalAggregate(keys=[{variables=[person1],"
                    + " aliases=[person1]}], values=[[{operands=[person2], aggFunction=COUNT,"
                    + " alias='p2Cnt', distinct=false}, {operands=[msg1], aggFunction=COUNT,"
                    + " alias='msg1Cnt', distinct=true}, {operands=[msg2], aggFunction=COUNT,"
                    + " alias='msg2Cnt', distinct=true}]])\n"
                    + "            GraphLogicalProject(person1=[person1], person2=[person2],"
                    + " msg1=[CASE(AND(=(msg.creationDate, ?0), =(tag.name, ?1)), 1, 0)],"
                    + " msg2=[CASE(AND(=(msg.creationDate, ?2), =(tag.name, ?3)), 1, 0)],"
                    + " isAppend=[false])\n"
                    + "              LogicalFilter(condition=[OR(AND(=(msg.creationDate, ?0),"
                    + " =(tag.name, ?1)), AND(=(msg.creationDate, ?2), =(tag.name, ?3)))])\n"
                    + "                MultiJoin(joinFilter=[=(person2, person2)],"
                    + " isFullOuterJoin=[false], joinTypes=[[INNER, INNER]],"
                    + " outerJoinConditions=[[NULL, NULL]], projFields=[[ALL, ALL]])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[person2], startAlias=[msg], opt=[OUT],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "                    CommonTableScan(table=[[common#-91045548]])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[person2], startAlias=[person1], opt=[BOTH],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "                    CommonTableScan(table=[[common#-91045548]])\n"
                    + "common#-91045548:\n"
                    + "GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[tag], startAlias=[msg], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[msg], startAlias=[person1], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi18_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (tag:TAG {name: $tag})<-[:HASINTEREST]-(person1:PERSON),\n"
                                    + "     "
                                    + " (person1:PERSON)-[:KNOWS]-(mutualFriend:PERSON)-[:KNOWS]-(person2:PERSON)-[:HASINTEREST]->(tag)\n"
                                    + "WHERE person1 <> person2\n"
                                    + "  AND NOT (person1)-[:KNOWS]-(person2)\n"
                                    + "RETURN \n"
                                    + "\tperson1.id AS person1Id,\n"
                                    + "  person2.id AS person2Id,\n"
                                    + "  count(DISTINCT mutualFriend) AS mutualFriendCount\n"
                                    + "ORDER BY\n"
                                    + "\tmutualFriendCount DESC,\n"
                                    + "  person1Id ASC,\n"
                                    + "  person2Id ASC\n"
                                    + "LIMIT 20;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[mutualFriendCount], sort1=[person1Id],"
                    + " sort2=[person2Id], dir0=[DESC], dir1=[ASC], dir2=[ASC], fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person1.id, person2.id],"
                    + " aliases=[person1Id, person2Id]}], values=[[{operands=[mutualFriend],"
                    + " aggFunction=COUNT, alias='mutualFriendCount', distinct=true}]])\n"
                    + "    LogicalJoin(condition=[AND(=(person1, person1), =(person2, person2))],"
                    + " joinType=[anti])\n"
                    + "      LogicalFilter(condition=[<>(person1, person2)])\n"
                    + "        MultiJoin(joinFilter=[=(person1, person1)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person1], startAlias=[mutualFriend], opt=[BOTH],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            CommonTableScan(table=[[common#41705485]])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASINTEREST]}], alias=[person1], startAlias=[tag], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            CommonTableScan(table=[[common#41705485]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person2], startAlias=[person1], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX])\n"
                    + "common#41705485:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[mutualFriend], startAlias=[person2], opt=[BOTH],"
                    + " physicalOpt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[person2], startAlias=[tag], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }
}
