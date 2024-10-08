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

import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.junit.Assert;
import org.junit.Test;

public class WhereTest {

    @Test
    public void where_1_test() {
        RelNode where =
                Utils.eval(
                                "Match (a)-[b]->(c) Where a.name = \"marko\" and b.weight < 2.0 or"
                                        + " c.age + 10 < a.age Return a, b, c")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], c=[c], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[OR(AND(=(a.name, _UTF-8'marko'), <(b.weight,"
                    + " 2.0E0)), <(+(c.age, 10), a.age))])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                where.explain().trim());
    }

    @Test
    public void where_2_test() {
        RelNode where =
                Utils.eval(
                                "Match (a) Where a.name = 'kli' and (a.age + 1 = 29 or a.name ="
                                        + " 'marko') Return a")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], fusedFilter=[[AND(=(_.name, _UTF-8'kli'), OR(=(+(_.age, 1), 29),"
                    + " =(_.name, _UTF-8'marko')))]], opt=[VERTEX])",
                where.explain().trim());
    }

    @Test
    public void where_3_test() {
        RelNode where =
                Utils.eval("Match (n:person) Where n.name = $name1 or n.name = $name2 Return n")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[n], fusedFilter=[[OR(=(_.name, ?0), =(_.name, ?1))]],"
                        + " opt=[VERTEX])",
                where.explain().trim());
    }

    @Test
    public void where_4_test() {
        RelNode where =
                Utils.eval(
                                "Match (n:person)-[]-(m:person) Where n.name = $name and m.name ="
                                        + " $name Return n")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[AND(=(n.name, ?0), =(m.name, ?0))])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[m], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                    + " opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[n], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                where.explain().trim());
    }

    @Test
    public void where_5_test() {
        RelNode where =
                Utils.eval(
                                "Match (a:person) Where (CASE WHEN a.name = 'marko' THEN 1 WHEN"
                                        + " a.age > 10 THEN 2 ELSE 3 END) > 2 Return a")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], fusedFilter=[[>(CASE(=(_.name, _UTF-8'marko'), 1,"
                        + " >(_.age, 10), 2, 3), 2)]], opt=[VERTEX])",
                where.explain().trim());
    }

    @Test
    public void where_6_test() {
        RelNode before =
                Utils.eval(
                                "Match (a:person)-[]->()-[]->(b:person) Where Not a=b AND NOT"
                                        + " (a:person)-[]->(b:person) Return a, b")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[<>(a, b)])\n"
                    + "    LogicalJoin(condition=[AND(=(a, a), =(b, b))], joinType=[anti])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[_],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                before.explain().trim());
    }

    // 'b is null' cannot be pushed down, for the conversion will change the semantics
    @Test
    public void where_7_test() {
        RelNode multiMatch =
                Utils.eval("Match (a) Optional Match (a)-[]->(b) Where b is null Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[IS NULL(b)])\n"
                    + "    LogicalJoin(condition=[=(a, a)], joinType=[left])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                multiMatch.explain().trim());
        RelOptPlanner optPlanner =
                com.alibaba.graphscope.common.ir.Utils.mockPlanner(
                        CoreRules.FILTER_INTO_JOIN.config, FilterMatchRule.Config.DEFAULT);
        optPlanner.setRoot(multiMatch);
        RelNode after = optPlanner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[IS NULL(b)])\n"
                    + "    LogicalJoin(condition=[=(a, a)], joinType=[left])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
    }

    // 'b is not null' can be pushed down
    @Test
    public void where_8_test() {
        RelNode multiMatch =
                Utils.eval("Match (a) Optional Match (a)-[]->(b) Where b is not null Return a")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[IS NOT NULL(b)])\n"
                    + "    LogicalJoin(condition=[=(a, a)], joinType=[left])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                multiMatch.explain().trim());

        RelOptPlanner optPlanner =
                com.alibaba.graphscope.common.ir.Utils.mockPlanner(
                        CoreRules.FILTER_INTO_JOIN.config, FilterMatchRule.Config.DEFAULT);
        optPlanner.setRoot(multiMatch);
        RelNode after = optPlanner.findBestExp();

        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  LogicalJoin(condition=[=(a, a)], joinType=[inner])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], fusedFilter=[[IS NOT NULL(_)]], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
    }

    @Test
    public void where_9_test() {
        RelNode where =
                Utils.eval(
                                "Match (a:software) With a.creationDate as creationDate Where"
                                    + " creationDate.month > 1990 and creationDate.day = 12 Return"
                                    + " creationDate")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(creationDate=[creationDate], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[AND(>(EXTRACT(FLAG(MONTH), creationDate), 1990),"
                    + " =(EXTRACT(FLAG(DAY), creationDate), 12))])\n"
                    + "    GraphLogicalProject(creationDate=[a.creationDate], isAppend=[false])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[software]}],"
                    + " alias=[a], opt=[VERTEX])",
                where.explain().trim());
    }

    // expect to throw exceptions
    @Test
    public void where_10_test() {
        try {
            RelNode where =
                    Utils.eval(
                                    "Match (a:software) With a.name as creationDate Where"
                                            + " creationDate.month > 1990 and creationDate.day = 12"
                                            + " Return creationDate")
                            .build();
        } catch (Exception e) {
            Assert.assertTrue(
                    e.getMessage()
                            .contains(
                                    "invalid property lookup operation, cannot get property or"
                                            + " extract interval from expr=[creationDate,"
                                            + " type=CHAR(1)]"));
            return;
        }
        Assert.fail("should have thrown exceptions for property 'name' is not a date type");
    }

    @Test
    public void where_11_test() {
        // the condition is fused into source and identified as primary key filtering
        RelNode where = Utils.eval("Match (a:person) Where elementId(a) = 2 Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX], uniqueKeyFilters=[=(_.~id, 2)])",
                where.explain().trim());
    }
}
