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

package com.alibaba.graphscope.common.calcite;

import com.alibaba.graphscope.common.calcite.tools.GraphBuilder;
import com.alibaba.graphscope.common.calcite.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.calcite.tools.config.LabelConfig;
import com.alibaba.graphscope.common.calcite.tools.config.SourceConfig;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExpressionTest {
    private GraphBuilder builder;

    @Before
    public void before() {
        this.builder = SourceTest.mockGraphBuilder();
    }

    @Test
    public void literal_test() {
        RexNode node = builder.literal(1);
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        node = builder.literal("s");
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.CHAR);
        node = builder.literal(true);
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        node = builder.literal(1.0);
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.DOUBLE);
    }

    @Test
    public void variable_1_test() {
        RexNode node = builder.source(mockSourceConfig(null)).variable((String) null);
        Assert.assertEquals(node.toString(), "DEFAULT");
    }

    @Test
    public void variable_2_test() {
        RexNode node = builder.source(mockSourceConfig("a")).variable("a");
        Assert.assertEquals(node.toString(), "a");
    }

    @Test
    public void variable_3_test() {
        try {
            builder.source(mockSourceConfig(null)).variable("a");
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail("tag 'a' should not exist");
    }

    @Test
    public void variable_4_test() {
        RexNode node = builder.source(mockSourceConfig(null)).variable(null, "name");
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.CHAR);
        Assert.assertEquals(node.toString(), "DEFAULT.name");
    }

    @Test
    public void variable_5_test() {
        RexNode node = builder.source(mockSourceConfig("a")).variable("a", "age");
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        Assert.assertEquals(node.toString(), "a.age");
    }

    // a.age + 10
    @Test
    public void plus_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode plus = builder.call(GraphStdOperatorTable.PLUS, var, builder.literal(10));
        Assert.assertEquals(plus.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        Assert.assertEquals(plus.toString(), "+(a.age, 10)");
    }

    // a.age == 10
    @Test
    public void equal_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode equal = builder.call(GraphStdOperatorTable.EQUALS, var, builder.literal(10));
        Assert.assertEquals(equal.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        Assert.assertEquals(equal.toString(), "=(a.age, 10)");
    }

    // a.age + 10 > 30
    @Test
    public void equal_plus_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode plus = builder.call(GraphStdOperatorTable.PLUS, var, builder.literal(10));
        RexNode equal = builder.call(GraphStdOperatorTable.EQUALS, plus, builder.literal(30));
        Assert.assertEquals(equal.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        Assert.assertEquals(equal.toString(), "=(+(a.age, 10), 30)");
    }

    // a.age > 10 && a.name == 'x'
    @Test
    public void and_test() {
        RexNode var1 = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode var2 = builder.variable("a", "name");
        RexNode equal1 =
                builder.call(GraphStdOperatorTable.GREATER_THAN, var1, builder.literal(10));
        RexNode equal2 = builder.call(GraphStdOperatorTable.EQUALS, var2, builder.literal("x"));
        RexNode node = builder.call(GraphStdOperatorTable.AND, equal1, equal2);
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        Assert.assertEquals(node.toString(), "AND(>(a.age, 10), =(a.name, 'x'))");
    }

    private SourceConfig mockSourceConfig(String alias) {
        return new SourceConfig().labels(new LabelConfig(false).addLabel("person")).alias(alias);
    }
}
