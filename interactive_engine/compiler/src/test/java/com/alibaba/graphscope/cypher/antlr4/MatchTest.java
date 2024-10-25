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
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MatchTest {
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
                        "config/modern/graph.yaml",
                        "statistics/modern_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void match_1_test() {
        RelNode source = Utils.eval("Match (n) Return n").build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[n], opt=[VERTEX])",
                source.explain().trim());
    }

    @Test
    public void match_2_test() {
        RelNode source =
                Utils.eval("Match (n:person)-[x:knows]->(y:person) Return n, x, y").build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], x=[x], y=[y], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[y], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[n], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                source.explain().trim());
    }

    @Test
    public void match_3_1_test() {
        // In the modern graph, there are only two kinds of edges,
        // one is `(person)-[knows]->(person)`, the other is `(person)-[created]->(software)`.
        // Thus, the type of a, b and c can be automatically inferred as follows:
        //  * b must be of type `person`, because only person can be the starting vertex
        //  * c can thus be either `person`/`software`, via the edge of `knows`/`created`
        //  * a can only be `person`, because only `person` can connect with another `person` vertex
        RelNode match = Utils.eval("Match (a)-[]->(b), (b)-[]->(c) Return a, b, c").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], c=[c], isAppend=[false])\n"
                    + "  GraphLogicalMultiMatch(input=[null],"
                    + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[VERTEX])\n"
                    + "]}])",
                match.explain().trim());
    }

    // if the type inference is disabled, the type will be the intersection of the query given types
    // and the overall possible types from schema
    @Test
    public void match_3_2_test() {
        // disable the type inference
        RelNode match =
                Utils.eval(
                                "Match (a)-[]->(b), (b)-[]->(c) Return a, b, c",
                                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(
                                        new Configs(
                                                ImmutableMap.of(
                                                        FrontendConfig.GRAPH_TYPE_INFERENCE_ENABLED
                                                                .getKey(),
                                                        "false"))))
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], c=[c], isAppend=[false])\n"
                    + "  GraphLogicalMultiMatch(input=[null],"
                    + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=true,"
                    + " tables=[software, person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[VERTEX])\n"
                    + "]}])",
                match.explain().trim());
    }

    @Test
    public void match_4_test() {
        // In the modern graph, there are only two kinds of edges,
        // one is `(person)-[knows]->(person)`, the other is `(person)-[created]->(software)`.
        // for the sentence `(a:person)-[b:knows*1..3]-(c:person)`:
        // b is a `path_expand` operator, expand base should be `knows` type, the associated vertex
        // can only be `person` type.
        RelNode match =
                Utils.eval(
                                "Match (a:person)-[b:knows*1..3 {weight:1.0}]->(c:person {name:"
                                        + " 'marko'}) Return a, b")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], fusedFilter=[[=(_.name, _UTF-8'marko')]], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[_], fusedFilter=[[=(_.weight, 1.0E0)]],"
                    + " opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[ALL_V_E],"
                    + " alias=[b])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                match.explain().trim());
    }

    @Test
    public void match_5_test() {
        RelNode match = Utils.eval("Match (n:person {age: $age}) Return n").build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[n], fusedFilter=[[=(_.age, ?0)]], opt=[VERTEX])",
                match.explain().trim());
    }

    @Test
    public void match_6_test() {
        RelNode project = Utils.eval("Match (a:person {id: 2l}) Return a").build();
        GraphLogicalSource source = (GraphLogicalSource) project.getInput(0);
        RexCall condition = (RexCall) source.getUniqueKeyFilters();
        Assert.assertEquals(
                SqlTypeName.BIGINT, condition.getOperands().get(1).getType().getSqlTypeName());
    }

    @Test
    public void match_7_test() {
        RelNode multiMatch =
                Utils.eval(
                                "Match (a:person)-[x:knows]->(b:person),"
                                        + " (b:person)-[:knows]-(c:person) Optional Match"
                                        + " (a:person)-[]->(c:person) Return a")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  LogicalJoin(condition=[AND(=(a, a), =(c, c))], joinType=[left])\n"
                    + "    GraphLogicalMultiMatch(input=[null],"
                    + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                    + " opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[VERTEX])\n"
                    + "]}])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}]," // `knows` is inferred
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])\n"
                        + "], matchOpt=[INNER])",
                multiMatch.explain().trim());
    }

    @Test
    public void match_8_test() {
        RelNode multiMatch = Utils.eval("Match (a) Where not (a)-[c]-(b) Return a Limit 1").build();
        // we convert the NOT MATCH to ANTI JOIN in GraphBuilder directly
        Assert.assertEquals(
                "GraphLogicalSort(fetch=[1])\n"
                    + "  GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "    LogicalJoin(condition=[=(a, a)], joinType=[anti])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[c], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                multiMatch.explain().trim());
    }

    @Test
    public void match_9_test() {
        LogicalPlan plan =
                Utils.evalLogicalPlan(
                        "Match (n:person {name: $name}) Where n.age = $age Return n.id;");
        Assert.assertEquals(
                "[Parameter{name='name', dataType=CHAR(1)}, Parameter{name='age',"
                        + " dataType=INTEGER}]",
                plan.getDynamicParams().toString());
        Assert.assertEquals("RecordType(BIGINT id)", plan.getOutputType().toString());
    }

    // add a new test case for match without any dynamic params
    @Test
    public void match_10_test() {
        LogicalPlan plan = Utils.evalLogicalPlan("Match (n:person) Return n.id;");
        Assert.assertTrue(plan.getDynamicParams().isEmpty());
        Assert.assertEquals("RecordType(BIGINT id)", plan.getOutputType().toString());
    }

    // add a new test case for match with multiple dynamic params
    @Test
    public void match_11_test() {
        LogicalPlan plan =
                Utils.evalLogicalPlan(
                        "Match (n:person {name: $name, age: $age}) Where n.id > 10 Return n.id,"
                                + " n.name;");
        Assert.assertEquals(
                "[Parameter{name='name', dataType=CHAR(1)}, Parameter{name='age',"
                        + " dataType=INTEGER}]",
                plan.getDynamicParams().toString());
        Assert.assertEquals("RecordType(BIGINT id, CHAR(1) name)", plan.getOutputType().toString());
    }

    @Test
    public void match_12_test() {
        try {
            RelNode node = Utils.eval("Match (a:人类) Return a").build();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Table \'人类\' not found"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void match_13_test() {
        try {
            RelNode node = Utils.eval("Match (a:person {名称:'marko'}) Return a").build();
        } catch (FrontendException e) {
            Assert.assertTrue(
                    e.getMessage()
                            .contains(
                                    "{property=名称} not found; expected properties are: [id, name,"
                                            + " age]"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void match_14_test() {
        RelNode node = Utils.eval("Match (a:person {name:'小明'}) Return '小明'").build();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_UTF-8'小明'], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], fusedFilter=[[=(_.name, _UTF-8'小明')]], opt=[VERTEX])",
                node.explain().trim());
        Assert.assertEquals(
                SqlTypeName.CHAR,
                node.getRowType().getFieldList().get(0).getType().getSqlTypeName());
    }

    @Test
    public void match_15_test() {
        RelNode node = Utils.eval("Match (a)-[b]-(c) Return labels(a), type(b)").build();
        Assert.assertEquals(
                "GraphLogicalProject(~label=[a.~label], ~label0=[b.~label], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                node.explain().trim());
    }

    @Test
    public void match_16_test() {
        RelNode node =
                Utils.eval(
                                "Match (a:person {name: $name})-[b]->(c:person {name: $name})"
                                        + " Return a, c")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], c=[c], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], fusedFilter=[[=(_.name, ?0)]], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[b]," // `knows` is inferred
                        + " opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])\n"
                        + "], matchOpt=[INNER])",
                node.explain().trim());
    }

    @Test
    public void match_17_test() {
        RelNode node =
                Utils.eval("Match (a:person) Where a.name starts with 'marko' Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], fusedFilter=[[POSIX REGEX CASE SENSITIVE(_.name,"
                        + " _UTF-8'^marko.*')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void match_18_test() {
        RelNode node =
                Utils.eval("Match (a:person) Where a.name ends with 'marko' Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], fusedFilter=[[POSIX REGEX CASE SENSITIVE(_.name,"
                        + " _UTF-8'.*marko$')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void match_19_test() {
        RelNode node =
                Utils.eval("Match (a:person) Where a.name contains 'marko' Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], fusedFilter=[[POSIX REGEX CASE SENSITIVE(_.name,"
                        + " _UTF-8'.*marko.*')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void match_20_test() {
        RelNode node =
                Utils.eval(
                                "Match (a:person)-[]->(b:person) Match (a:person)-[]-(c:person),"
                                        + " (c:person)-[]->(b:person) Return a, b")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], isAppend=[false])\n"
                    + "  LogicalJoin(condition=[AND(=(a, a), =(b, b))], joinType=[inner])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}]," // `knows` is inferred
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])\n"
                        + "], matchOpt=[INNER])\n"
                        + "    GraphLogicalMultiMatch(input=[null],"
                        + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=false,"
                        + " tables=[person]}], alias=[c], opt=[OTHER])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}]," // `knows` is inferred
                        + " alias=[_], opt=[BOTH])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])\n"
                        + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[b], opt=[END])\n"
                        + "  GraphLogicalExpand(tableConfig=[{isAll=false,"
                        + " tables=[knows]}]," // `knows` is inferred
                        + " alias=[_], opt=[OUT])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[c], opt=[VERTEX])\n"
                        + "]}])",
                node.explain().trim());
    }

    @Test
    public void match_21_test() {
        // The IN operator in Cypher will be transformed into the following three logical plans:
        // 1) IN ['marko', 'vadas'], where the elements in the list consist only of constants, will
        // be converted into a SEARCH operator.
        RelNode node = Utils.eval("Match (a) Where a.name IN ['marko', 'vadas'] Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], fusedFilter=[[SEARCH(_.name, Sarg[_UTF-8'marko',"
                    + " _UTF-8'vadas']:CHAR(5) CHARACTER SET \"UTF-8\")]], opt=[VERTEX])",
                node.explain().trim());

        // 2) IN [a.age, 1], where the elements include variables, will be decomposed into a set of
        // OR predicates.
        node = Utils.eval("Match (a) Where a.id IN [a.age, 1] Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], fusedFilter=[[OR(=(_.id, _.age), =(_.id, 1))]], opt=[VERTEX])",
                node.explain().trim());

        // 3) Dynamic parameters will be transformed into the IN operator.
        // The differences between 1) and 3) exist only in the logical plan, as both will be
        // converted into WITHIN in the physical plan.
        node = Utils.eval("Match (a) Where a.name IN $names Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], fusedFilter=[[IN(_.name, ?0)]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void match_22_test() {
        RelNode node =
                Utils.eval(
                                "Match (a)-[b]-(c) Return (a.creationDate - c.creationDate) / 1000"
                                        + " as diff")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(diff=[/(DATETIME_MINUS(a.creationDate, c.creationDate,"
                    + " null:INTERVAL MILLISECOND), 1000)], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                node.explain().trim());
    }

    @Test
    public void match_23_test() {
        RelNode node =
                Utils.eval(
                                "Match (a)-[b]-(c) Return a.creationDate + duration({years: $year,"
                                        + " months: $month})")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[+(a.creationDate, +(?0, ?1))], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                node.explain().trim());
    }

    // test type inference of path expand, a can reach b through the following two paths: either 0
    // or 1 edge(s)
    // 1. (a)-[*0]->(b) -> (a:software)-[*0]->(b:software) or (a:person)-[*0]->(b:person)
    // 2. (a)-[*1]->(b) -> (a:person)-[:created]->(b:software) or (a:person)-[:knows]->(b:person)
    @Test
    public void match_24_test() {
        RelNode node = Utils.eval("Match (a)-[c*0..2]->(b) Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=true,"
                    + " tables=[created, knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[END])\n"
                    + "], fetch=[2], path_opt=[ARBITRARY], result_opt=[ALL_V_E], alias=[c])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                node.explain().trim());
    }

    @Test
    public void property_exist_after_type_inference_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(
                        "schema/ldbc_schema_exp_hierarchy.json");
        // check property 'creationDate' still exists after type inference has updated the type of
        // 'HASCREATOR'
        RelNode rel =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a:PERSON)<-[h:HASCREATOR]-(b:COMMENT) Return h;", builder)
                        .build();
        Assert.assertEquals(
                "RecordType(Graph_Schema_Type(labels=[EdgeLabel(HASCREATOR, COMMENT, PERSON)],"
                        + " properties=[BIGINT creationDate]) h)",
                rel.getRowType().toString());
    }

    @Test
    public void udf_function_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode node =
                Utils.eval(
                                "MATCH (person1:person)-[path:knows]->(person2:person)\n"
                                        + " Return gs.function.startNode(path)",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject($f0=[gs.function.startNode(path)], isAppend=[false])\n"
                        + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[person2], opt=[END])\n"
                        + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[path], startAlias=[person1], opt=[OUT])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[person1], opt=[VERTEX])",
                after.explain().trim());

        RelNode node2 =
                Utils.eval(
                                "Match (person)-[:created]->(software) WITH software.creationDate"
                                    + " as date1\n"
                                    + "Return 12 * (date1.year - gs.function.datetime($date2).year)"
                                    + " + (date1.month - gs.function.datetime($date2).month)",
                                builder)
                        .build();
        RelNode after2 = optimizer.optimize(node2, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject($f0=[+(*(12, -(EXTRACT(FLAG(YEAR), date1), EXTRACT(FLAG(YEAR),"
                    + " gs.function.datetime(?0)))), -(EXTRACT(FLAG(MONTH), date1),"
                    + " EXTRACT(FLAG(MONTH), gs.function.datetime(?0))))], isAppend=[false])\n"
                    + "  GraphLogicalProject(date1=[software.creationDate], isAppend=[false])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[created]}],"
                    + " alias=[software], startAlias=[person], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[person], opt=[VERTEX])",
                after2.explain().trim());

        RelNode node3 =
                Utils.eval(
                                "Match (person:person)\n"
                                        + "Return gs.function.toFloat(person.age)",
                                builder)
                        .build();
        RelNode after3 = optimizer.optimize(node3, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject($f0=[gs.function.toFloat(person.age)], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[person], opt=[VERTEX])",
                after3.explain().trim());
    }

    @Test
    public void procedure_shortest_path_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        LogicalPlanVisitor logicalPlanVisitor = new LogicalPlanVisitor(builder, irMeta);
        LogicalPlan logicalPlan =
                logicalPlanVisitor.visit(
                        new CypherAntlr4Parser()
                                .parse(
                                        "MATCH\n"
                                            + "(person1:person {id:"
                                            + " $person1Id})-[:knows]->(person2:person {id:"
                                            + " $person2Id})\n"
                                            + "CALL shortestPath.dijkstra.stream(\n"
                                            + "  person1, person2, 'KNOWS', 'BOTH', 'weight', 5)\n"
                                            + "WITH person1, person2, totalCost\n"
                                            + "WHERE person1.id <> $person2Id\n"
                                            + "Return person1.id AS person1Id, totalCost AS"
                                            + " totalWeight;"));
        RelNode after =
                optimizer.optimize(
                        logicalPlan.getRegularQuery(), new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(person1Id=[person1.id], totalWeight=[totalCost],"
                    + " isAppend=[false])\n"
                    + "  LogicalFilter(condition=[<>(person1.id, ?1)])\n"
                    + "    GraphLogicalProject(person1=[person1], person2=[person2],"
                    + " totalCost=[totalCost], isAppend=[false])\n"
                    + "      GraphProcedureCall(procedure=[shortestPath.dijkstra.stream(person1,"
                    + " person2, _UTF-8'KNOWS', _UTF-8'BOTH', _UTF-8'weight', 5)])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[person2], fusedFilter=[[=(_.id, ?1)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[_], startAlias=[person1], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[person1], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])",
                after.explain().trim());
    }

    @Test
    public void shortest_path_test() {
        // convert 'shortestpath' modifier to 'path_opt=[ANY_SHORTEST]' in IR, and 'all
        // shortestpath' to 'path_opt=[ALL_SHORTEST]'
        RelNode rel =
                Utils.eval(
                                "MATCH"
                                    + " shortestPath((person1:person)-[path:knows*1..5]->(person2:person))"
                                    + " Return count(person1)")
                        .build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[person1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[person2], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "], offset=[1], fetch=[4], path_opt=[ANY_SHORTEST], result_opt=[ALL_V_E],"
                    + " alias=[path])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[person1], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                rel.explain().trim());
    }

    @Test
    public void optional_shortest_path_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode node =
                Utils.eval(
                                "Match (p1: person {id: $id1})\n"
                                        + "Optional Match shortestPath((p1:person {id:"
                                        + " $id1})-[k:knows*1..5]->(p2:person {id: $id2}))\n"
                                        + "WITH\n"
                                        + "CASE WHEN k is null then -1\n"
                                        + "ELSE length(k)\n"
                                        + "END as len\n"
                                        + "RETURN len;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(len=[len], isAppend=[false])\n"
                    + "  GraphLogicalProject(len=[CASE(IS NULL(k), -(1), k.~len)],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[p2], fusedFilter=[[=(_.id, ?1)]], opt=[END])\n"
                    + "     "
                    + " GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "], offset=[1], fetch=[4], path_opt=[ANY_SHORTEST], result_opt=[ALL_V_E],"
                    + " alias=[k], start_alias=[p1], optional=[true])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[p1], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])",
                after.explain().trim());
    }
}
