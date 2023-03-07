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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class WithTest {
    private GraphBuilder eval(String query) {
        return CypherUtils.mockVisitor(CypherUtils.mockGraphBuilder())
                .visitOC_With(CypherUtils.mockParser(query).oC_With());
    }

    // project(a) -> expr: a, alias: a
    @Test
    public void with_test_1() {
        RelNode project = eval("With a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    // project(a.age) -> expr: a.age, alias: age
    @Test
    public void with_test_2() {
        RelNode project = eval("With a.age").build();
        Assert.assertEquals(
                "GraphLogicalProject(age=[a.age], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
        Assert.assertEquals("RecordType(INTEGER age)", project.getRowType().toString());
    }

    // project(a.name, b as d) -> {expr: a.name, alias: name}, {expr: b, alias: d}
    @Test
    public void with_test_3() {
        RelNode project = eval("With a.name, b as d").build();
        Assert.assertEquals(
                "GraphLogicalProject(name=[a.name], d=[b], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
        Assert.assertEquals(
                "RecordType(CHAR(1) name, Graph_Schema_Type(DOUBLE weight) d)",
                project.getRowType().toString());
    }

    // group by a.name, count(a.name)
    @Test
    public void with_test_4() {
        RelNode project = eval("With a.name, count(a.name)").build();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[null]}],"
                    + " values=[[{operands=[a.name], aggFunction=COUNT, alias='null'}]])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
        Assert.assertEquals(
                "RecordType(CHAR(1) name, BIGINT $f1)", project.getRowType().toString());
    }

    // group by a.name, count(a.name) + 1 as d -> aggregate + project
    @Test
    public void with_test_5() {
        RelNode project = eval("With a.name as name, count(a.name) + 1 as d").build();
        Assert.assertEquals(
                "GraphLogicalProject(d=[+(EXPR$0., 1)], isAppend=[true])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[name]}],"
                    + " values=[[{operands=[a.name], aggFunction=COUNT, alias='EXPR$0'}]])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }

    // project + order + limit -> project + topK
    @Test
    public void with_test_6() {
        RelNode project = eval("With a.name as name Order by name desc Limit 10").build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[name], dir0=[DESC], fetch=[10])\n"
                    + "  GraphLogicalProject(name=[a.name], isAppend=[false])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                project.explain().trim());
    }
}
