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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class CaseTest {
    // case when a.name = 'marko' then 1 else 3 end
    @Test
    public void case_1_test() {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"));
        RexNode caseExpr =
                builder.call(
                        GraphStdOperatorTable.CASE,
                        builder.call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable("a", "name"),
                                builder.literal("marko")),
                        builder.literal(1),
                        builder.literal(3));
        Assert.assertEquals("CASE(=(a.name, _UTF-8'marko'), 1, 3)", caseExpr.toString());
        Assert.assertEquals(SqlTypeName.INTEGER, caseExpr.getType().getSqlTypeName());
    }

    // test case when containing dynamic parameters: case when a.name = 'marko' then ? else 3 end
    @Test
    public void case_2_test() {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"));
        GraphRexBuilder rexBuilder = (GraphRexBuilder) builder.getRexBuilder();
        RexNode caseExpr =
                builder.call(
                        GraphStdOperatorTable.CASE,
                        builder.call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable("a", "name"),
                                builder.literal("marko")),
                        rexBuilder.makeGraphDynamicParam("value", 0),
                        builder.literal(3));
        Assert.assertEquals("CASE(=(a.name, _UTF-8'marko'), ?0, 3)", caseExpr.toString());
        Assert.assertEquals(SqlTypeName.INTEGER, caseExpr.getType().getSqlTypeName());
        Assert.assertEquals(
                SqlTypeName.INTEGER,
                ((RexCall) caseExpr).getOperands().get(1).getType().getSqlTypeName());
    }
}
