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

package com.alibaba.graphscope.cypher.antlr4.write;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteOpTest {
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
                com.alibaba.graphscope.common.ir.Utils.mockIrMeta(
                        "schema/ldbc_schema_exp_hierarchy.json",
                        "statistics/ldbc30_hierarchy_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void load_csv_create_vertex_test() {
        String query =
                "LOAD CSV FROM $csv_file AS row FIELDTERMINATOR '|'\n"
                        + "CREATE (forum:FORUM {id: row[1]});";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Vertex{table=[FORUM],"
                        + " mappings=FieldMappings{mappings=[Entry{source=row[1], target=_.id}]}}],"
                        + " alias=[forum])\n"
                        + "  LoadCSVTableScan(tableConfig=[{isAll=false, tables=[csv_file]}],"
                        + " alias=[row])",
                after.explain().trim());
    }

    @Test
    public void load_csv_create_edge_test() {
        String query =
                "LOAD CSV FROM \"xxx/*.csv.gz\" AS row FIELDTERMINATOR '|'\n"
                        + "CREATE (p1:PERSON {id: row[0]})-[k:KNOWS {creationDate:"
                        + " row[1]}]->(p2:PERSON {id: row[2]}) Return k;";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(k=[k], isAppend=[false])\n"
                    + "  Insert(operation=[INSERT], target=[Edge{table=[KNOWS],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[1],"
                    + " target=_.creationDate}]}, srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[0], target=_.id}]}},"
                    + " dstVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[2], target=_.id}]}}}],"
                    + " alias=[k])\n"
                    + "    LoadCSVTableScan(tableConfig=[{isAll=false, tables=[xxx/*.csv.gz]}],"
                    + " alias=[row])",
                after.explain().trim());
    }

    @Test
    public void load_csv_match_vertex_test() {
        String query =
                "LOAD CSV FROM \"xxx/*.csv.gz\" AS row FIELDTERMINATOR '|' "
                        + "MATCH (forum:FORUM {id: row[1]})\n"
                        + "return forum";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(forum=[forum], isAppend=[false])\n"
                    + "  DataSourceTableScan(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], target=[Vertex{table=[FORUM],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[1], target=_.id}]}}])\n"
                    + "    LoadCSVTableScan(tableConfig=[{isAll=false, tables=[xxx/*.csv.gz]}],"
                    + " alias=[row])",
                after.explain().trim());
    }

    @Test
    public void load_csv_match_vertex_2_test() {
        String query =
                "LOAD CSV FROM $csv_file AS row FIELDTERMINATOR '|'\n"
                        + "CREATE (:COMMENT {id: row[1]})-[:HASCREATOR]->(:PERSON {id: row[2]})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[HASCREATOR],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[1], target=_.id}]}},"
                    + " dstVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[2], target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  LoadCSVTableScan(tableConfig=[{isAll=false, tables=[csv_file]}],"
                    + " alias=[row])",
                after.explain().trim());
    }

    @Test
    public void load_csv_delete_vertex_test() {
        String query =
                "LOAD CSV FROM $csv_file AS row FIELDTERMINATOR '|'\n"
                    + "MATCH (person:PERSON {id: row[1]})\n"
                    + "OPTIONAL MATCH"
                    + " (person)<-[:HASCREATOR]-(:COMMENT)<-[:REPLYOF*0..10]-(message1:COMMENT)\n"
                    + "OPTIONAL MATCH (person)<-[:HASMODERATOR]-(forum:FORUM)\n"
                    + "WHERE forum.title STARTS WITH 'Album '\n"
                    + "   OR forum.title STARTS WITH 'Wall '\n"
                    + "OPTIONAL MATCH"
                    + " (forum)-[:CONTAINEROF]->(:POST)<-[:REPLYOF*0..10]-(message2:COMMENT)\n"
                    + "DETACH DELETE person, forum, message1, message2\n"
                    + "RETURN count(person)";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  Delete(operation=[DELETE], deleteExprs=[[person, forum, message1,"
                    + " message2]])\n"
                    + "    LogicalJoin(condition=[=(forum, forum)], joinType=[left])\n"
                    + "      LogicalFilter(condition=[OR(POSIX REGEX CASE SENSITIVE(forum.title,"
                    + " _UTF-8'^Album .*'), POSIX REGEX CASE SENSITIVE(forum.title, _UTF-8'^Wall"
                    + " .*'))])\n"
                    + "        LogicalJoin(condition=[=(person, person)], joinType=[left])\n"
                    + "          LogicalJoin(condition=[=(person, person)], joinType=[left])\n"
                    + "            DataSourceTableScan(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], target=[Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[1], target=_.id}]}}])\n"
                    + "              LoadCSVTableScan(tableConfig=[{isAll=false,"
                    + " tables=[csv_file]}], alias=[row])\n"
                    + "            GraphLogicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[message1], opt=[END])\n"
                    + "             "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF,"
                    + " COMMENT, COMMENT)]], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[10], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[PATTERN_VERTEX$1])\n"
                    + "                GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[COMMENT]}], alias=[PATTERN_VERTEX$1], opt=[START],"
                    + " physicalOpt=[ITSELF])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR,"
                    + " COMMENT, PERSON)]], alias=[_], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                    GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person], opt=[VERTEX])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMODERATOR]}], alias=[forum], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[message2], opt=[END])\n"
                    + "       "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[10], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[PATTERN_VERTEX$1])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[PATTERN_VERTEX$1], startAlias=[forum],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void load_csv_delete_vertex_2_test() {
        String query =
                "LOAD CSV FROM $csv_file AS row FIELDTERMINATOR '|'\n"
                    + "MATCH (person:PERSON {id: row[1]})\n"
                    + "OPTIONAL MATCH (person)<-[:HASMODERATOR]-(forum:FORUM) Where forum.title"
                    + " STARTS WITH 'Album' or forum is NULL\n"
                    + "OPTIONAL MATCH"
                    + " (forum)-[:CONTAINEROF]->(:POST)<-[:REPLYOF*0..10]-(message:COMMENT|POST)\n"
                    + "DETACH DELETE person, forum, message";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Delete(operation=[DELETE], deleteExprs=[[person, forum, message]])\n"
                    + "  LogicalJoin(condition=[=(forum, forum)], joinType=[left])\n"
                    + "    LogicalFilter(condition=[OR(POSIX REGEX CASE SENSITIVE(forum.title,"
                    + " _UTF-8'^Album.*'), IS NULL(forum))])\n"
                    + "      LogicalJoin(condition=[=(person, person)], joinType=[left])\n"
                    + "        DataSourceTableScan(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], target=[Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=row[1], target=_.id}]}}])\n"
                    + "          LoadCSVTableScan(tableConfig=[{isAll=false, tables=[csv_file]}],"
                    + " alias=[row])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMODERATOR]}], alias=[forum], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[VERTEX])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], opt=[END])\n"
                    + "     "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[10], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[PATTERN_VERTEX$1])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[PATTERN_VERTEX$1], startAlias=[forum],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void set_vertex_test() {
        String query =
                "MATCH"
                    + " (country:COUNTRY)<-[:ISPARTOF]-(:CITY)<-[:ISLOCATEDIN]-(person:PERSON)<-[:HASMEMBER]-(forum:FORUM)\n"
                    + "WITH\n"
                    + "  forum,\n"
                    + "  country,\n"
                    + "  count(person) as personCount\n"
                    + "WITH\n"
                    + "  forum,\n"
                    + "  max(personCount) as popularCount\n"
                    + "SET forum.id = popularCount;";

        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Update(operation=[UPDATE],"
                    + " updateMappings=[FieldMappings{mappings=[Entry{source=popularCount,"
                    + " target=forum.id}]}])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[forum], aliases=[forum]}],"
                    + " values=[[{operands=[personCount], aggFunction=MAX, alias='popularCount',"
                    + " distinct=false}]])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[forum, country], aliases=[forum,"
                    + " country]}], values=[[{operands=[person], aggFunction=COUNT,"
                    + " alias='personCount', distinct=false}]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[forum], startAlias=[person], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[START], physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " CITY)]], alias=[_], startAlias=[PATTERN_VERTEX$1], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISPARTOF, CITY,"
                    + " COUNTRY)]], alias=[PATTERN_VERTEX$1], startAlias=[country], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[COUNTRY]}], alias=[country], opt=[VERTEX])",
                after.explain().trim());
    }
}
