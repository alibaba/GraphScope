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

import com.alibaba.graphscope.calcite.antlr4.visitor.CypherToAlgebraVisitor;
import com.alibaba.graphscope.calcite.antlr4.visitor.ExpressionVisitor;
import com.alibaba.graphscope.common.ir.SourceTest;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionTest {
    private ExpressionVisitor mockExpressionVisitor() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "b"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "c"))
                        .build();
        builder.match(sentence, GraphOpt.Match.INNER);
        return new ExpressionVisitor(new CypherToAlgebraVisitor(builder));
    }

    private RexNode eval(String query) {
        return mockExpressionVisitor()
                .visitOC_Expression(MatchTest.parser(query).oC_Expression())
                .getExpr();
    }

    @Test
    public void expr_test_1() {
        RexNode node = eval("a.name = 'kli' and (a.age + 1 = 29 or a.name = 'marko')");
        Assert.assertEquals(
                "AND(=(a.name, 'kli'), OR(=(+(a.age, 1), 29), =(a.name, 'marko')))",
                node.toString());
        Assert.assertEquals(SqlTypeName.BOOLEAN, node.getType().getSqlTypeName());
    }

    @Test
    public void expr_test_2() {
        RexNode node = eval("a.age >= 10");
        Assert.assertEquals(">=(a.age, 10)", node.toString());
        Assert.assertEquals(SqlTypeName.BOOLEAN, node.getType().getSqlTypeName());
    }

    @Test
    public void expr_test_3() {
        RexNode node = eval("a.age + (10 - b.weight)");
        Assert.assertEquals("+(a.age, -(10, b.weight))", node.toString());
        Assert.assertEquals(SqlTypeName.DOUBLE, node.getType().getSqlTypeName());
    }
}
