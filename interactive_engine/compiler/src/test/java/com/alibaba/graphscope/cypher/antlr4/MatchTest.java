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

import com.alibaba.graphscope.common.ir.IrUtils;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class MatchTest {
    private GraphBuilder eval(String query) {
        return CypherUtils.mockVisitor(IrUtils.mockGraphBuilder())
                .visitOC_Match(CypherUtils.mockParser(query).oC_Match());
    }

    @Test
    public void match_test_1() {
        RelNode source = eval("Match (n)").build();
        Assert.assertEquals(
                "GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                        + " alias=[n], opt=[VERTEX])",
                source.explain().trim());
    }

    @Test
    public void match_test_2() {
        RelNode source = eval("Match (n:person)-[x:knows]->(y:person)").build();
        Assert.assertEquals(
                "GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[y], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[n], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                source.explain().trim());
    }
}
