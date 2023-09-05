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

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class WithTest {
    // project(a.age) -> expr: a.age, alias: age
    @Test
    public void with_1_test() {
        RelNode project = Utils.eval("Match (a) Return a.age").build();
        Assert.assertEquals(
                "GraphLogicalProject(age=[a.age], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // project(a.name, b as d) -> {expr: a.name, alias: name}, {expr: b, alias: d}
    @Test
    public void with_2_test() {
        RelNode project = Utils.eval("Match (a)-[b]->() Return a.name, b as d").build();
        Assert.assertEquals(
                "GraphLogicalProject(name=[a.name], d=[b], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    @Test
    public void with_3_test() {
        RelNode project =
                Utils.eval("Match (a)-[b]-() Return a.age + (10 - b.weight) as c").build();
        Assert.assertEquals(
                "GraphLogicalProject(c=[+(a.age, -(10, b.weight))], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    // group by a.name, count(a.name)
    @Test
    public void with_4_test() {
        RelNode aggregate = Utils.eval("Match (a) Return a.name, count(a.name) as b").build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[name]}],"
                    + " values=[[{operands=[a.name], aggFunction=COUNT, alias='b',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                aggregate.explain().trim());
    }

    // group by a.name, count(a.name) + 1 as d -> aggregate + project
    @Test
    public void with_5_test() {
        RelNode project =
                Utils.eval("Match (a) Return a.name as name, count(a.name) + 1 as d").build();
        Assert.assertEquals(
                "GraphLogicalProject(name=[EXPR$1], d=[+(EXPR$0, 1)], isAppend=[false])\n"
                        + "  GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[EXPR$1]}],"
                        + " values=[[{operands=[a.name], aggFunction=COUNT, alias='EXPR$0',"
                        + " distinct=false}]])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // distinct keys
    @Test
    public void with_6_test() {
        RelNode project = Utils.eval("Match (a) Return distinct a.name, a.age").build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[a.name, a.age], aliases=[name, age]}],"
                    + " values=[[]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // test simple case expression
    @Test
    public void with_7_test() {
        RelNode project =
                Utils.eval(
                                "Match (a:person) Return CASE a.name WHEN 'marko' THEN 1 WHEN"
                                        + " 'vadas' THEN 2 ELSE 3 END as d")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(d=[CASE(=(a.name, 'marko'), 1, =(a.name, 'vadas'), 2, 3)],"
                        + " isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // test searched case expression
    @Test
    public void with_8_test() {
        RelNode project =
                Utils.eval(
                                "Match (a:person) Return CASE WHEN a.name = 'marko' THEN 1 WHEN"
                                        + " a.age > 10 THEN 2 ELSE 3 END as d")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(d=[CASE(=(a.name, 'marko'), 1, >(a.age, 10), 2, 3)],"
                        + " isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // else expression is omitted: case a.name when 'marko' then 1 when 'josh' then 2 end
    @Test
    public void with_9_test() {
        RelNode project =
                Utils.eval(
                                "Match (a:person) Return CASE a.name WHEN 'marko' THEN 1 WHEN"
                                        + " 'josh' THEN 2 END as d")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(d=[CASE(=(a.name, 'marko'), 1, =(a.name, 'josh'), 2,"
                        + " null:NULL)], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // with + optional match
    @Test
    public void with_10_test() {
        RelNode project =
                Utils.eval(
                                "Match (a:person)-[]->(b:person) With b Optional Match"
                                        + " (b:person)-[]->(c:person) Return b, c")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(b=[b], c=[c], isAppend=[false])\n"
                    + "  LogicalJoin(condition=[=(b, b)], joinType=[left])\n"
                    + "    GraphLogicalProject(b=[b], isAppend=[false])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    @Test
    public void with_11_test() {
        RelNode project =
                Utils.eval("Match (a:person)-[k:knows*1..2]->(b:person) Return length(k)").build();
        Assert.assertEquals(
                "GraphLogicalProject(~len=[k.~len], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[END])\n"
                    + "], offset=[1], fetch=[1], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[k])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    @Test
    public void with_12_test() {
        RelNode project =
                Utils.eval("Match (a:person)-[]-(b:person) Return [a.name, b.age, 1]").build();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[ARRAY_VALUE_CONSTRUCTOR(a.name, b.age, 1)],"
                    + " isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[b], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    @Test
    public void with_13_test() {
        RelNode project =
                Utils.eval("Match (a:person) Return head(collect(a.name)) as name").build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                        + " values=[[{operands=[a.name], aggFunction=FIRST_VALUE, alias='name',"
                        + " distinct=false}]])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                project.explain().trim());
        Assert.assertEquals("RecordType(CHAR(1) name)", project.getRowType().toString());
    }
}
