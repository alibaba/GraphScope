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

public class WhereTest {

    @Test
    public void where_test_1() {
        RelNode where =
                CypherUtils.eval("Match (a)-[b]->(c) Where a.name = \"marko\" and b.weight < 2.0 or c.age + 10 < a.age Return a, b, c").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], c=[c], isAppend=[false])\n" +
                        "  LogicalFilter(condition=[OR(AND(=(a.name, 'marko'), <(b.weight, 2.0E0)), <(+(c.age, 10), a.age))])\n" +
                        "    GraphLogicalSingleMatch(input=[null], sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}], alias=[c], opt=[END])\n" +
                        "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}], alias=[b], opt=[OUT])\n" +
                        "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], opt=[VERTEX])\n" +
                        "], matchOpt=[INNER])",
                where.explain().trim());
    }

    @Test
    public void where_test_2() {
        RelNode where =
                CypherUtils.eval("Match (a) Where a.name = 'kli' and (a.age + 1 = 29 or a.name = 'marko') Return a").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n" +
                        "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}], alias=[a], fusedFilter=[[AND(=(a.name, 'kli'), OR(=(+(a.age, 1), 29), =(a.name, 'marko')))]], opt=[VERTEX])",
                where.explain().trim());
    }
}
