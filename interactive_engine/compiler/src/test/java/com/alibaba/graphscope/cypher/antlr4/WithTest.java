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
    public void with_test_1() {
        RelNode project = CypherUtils.eval("Match (a) Return a.age").build();
        Assert.assertEquals(
                "GraphLogicalProject(age=[a.age], isAppend=[false])\n" +
                        "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }

    // project(a.name, b as d) -> {expr: a.name, alias: name}, {expr: b, alias: d}
    @Test
    public void with_test_2() {
        RelNode project = CypherUtils.eval("Match (a)-[b]->() Return a.name, b as d").build();
        Assert.assertEquals(
                "GraphLogicalProject(name=[a.name], d=[b], isAppend=[false])\n" +
                        "  GraphLogicalSingleMatch(input=[null], sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}], alias=[~DEFAULT], opt=[END])\n" +
                        "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}], alias=[b], opt=[OUT])\n" +
                        "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], opt=[VERTEX])\n" +
                        "], matchOpt=[INNER])",
                project.explain().trim());
    }

    @Test
    public void with_test_3() {
        RelNode project = CypherUtils.eval("Match (a)-[b]-() Return a.age + (10 - b.weight) as c").build();
        Assert.assertEquals("GraphLogicalProject(c=[+(a.age, -(10, b.weight))], isAppend=[false])\n" +
                "  GraphLogicalSingleMatch(input=[null], sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}], alias=[~DEFAULT], opt=[BOTH])\n" +
                "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}], alias=[b], opt=[BOTH])\n" +
                "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], opt=[VERTEX])\n" +
                "], matchOpt=[INNER])", project.explain().trim());
    }

    // group by a.name, count(a.name)
    @Test
    public void with_test_4() {
        RelNode aggregate = CypherUtils.eval("Match (a) Return a.name, count(a.name) as b").build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[null]}], values=[[{operands=[a.name], aggFunction=COUNT, alias='b'}]])\n" +
                        "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], opt=[VERTEX])",
                aggregate.explain().trim());
    }

    // group by a.name, count(a.name) + 1 as d -> aggregate + project
    @Test
    public void with_test_5() {
        RelNode project = CypherUtils.eval("Match (a) Return a.name as name, count(a.name) + 1 as d").build();
        Assert.assertEquals(
                "GraphLogicalProject(d=[+(EXPR$0., 1)], isAppend=[true])\n" +
                        "  GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[name]}], values=[[{operands=[a.name], aggFunction=COUNT, alias='EXPR$0'}]])\n" +
                        "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }
}
