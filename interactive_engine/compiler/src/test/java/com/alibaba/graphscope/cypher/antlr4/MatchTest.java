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

public class MatchTest {
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
    public void match_3_test() {
        RelNode match = Utils.eval("Match (a)-[]->(b), (b)-[]->(c) Return a, b, c").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], c=[c], isAppend=[false])\n"
                    + "  GraphLogicalMultiMatch(input=[null],"
                    + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=true,"
                    + " tables=[software, person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[VERTEX])\n"
                    + "]}])",
                match.explain().trim());
    }
}
