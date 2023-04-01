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

public class OrderTest {

    @Test
    public void order_1_test() {
        RelNode order = Utils.eval("Match (a) Return a Order By a.name desc").build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[a.name], dir0=[DESC])\n"
                        + "  GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                order.explain().trim());
    }

    @Test
    public void order_2_test() {
        RelNode order =
                Utils.eval("Match (a)-[b]-() Return a, b Order By a.name desc, b.weight asc")
                        .build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[a.name], sort1=[b.weight], dir0=[DESC], dir1=[ASC])\n"
                    + "  GraphLogicalProject(a=[a], b=[b], isAppend=[false])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[OTHER])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                order.explain().trim());
    }

    // order + limit -> topK
    @Test
    public void order_3_test() {
        RelNode project =
                Utils.eval("Match (a) Return a.name as b Order by b desc Limit 10").build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[b], dir0=[DESC], fetch=[10])\n"
                        + "  GraphLogicalProject(b=[a.name], isAppend=[false])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                project.explain().trim());
    }
}
