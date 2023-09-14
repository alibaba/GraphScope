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
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExpressionTest {
    private GraphBuilder builder;

    @Before
    public void before() {
        this.builder = Utils.mockGraphBuilder();
    }

    @Test
    public void literal_test() {
        RexNode node = builder.literal(1);
        Assert.assertEquals(SqlTypeName.INTEGER, node.getType().getSqlTypeName());
        node = builder.literal("s");
        Assert.assertEquals(SqlTypeName.CHAR, node.getType().getSqlTypeName());
        node = builder.literal(true);
        Assert.assertEquals(SqlTypeName.BOOLEAN, node.getType().getSqlTypeName());
        node = builder.literal(1.0);
        Assert.assertEquals(SqlTypeName.DOUBLE, node.getType().getSqlTypeName());
    }

    // head
    @Test
    public void variable_1_test() {
        RexNode node = builder.source(mockSourceConfig(null)).variable((String) null);
        Assert.assertEquals("DEFAULT", node.toString());
    }

    // a
    @Test
    public void variable_2_test() {
        RexNode node = builder.source(mockSourceConfig("a")).variable("a");
        Assert.assertEquals("a", node.toString());
    }

    // non-exist tag -> throw errors
    @Test
    public void variable_3_test() {
        try {
            builder.source(mockSourceConfig(null)).variable("a");
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail("tag 'a' does not exist");
    }

    // head.name
    @Test
    public void variable_4_test() {
        RexNode node = builder.source(mockSourceConfig(null)).variable(null, "name");
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.CHAR);
        Assert.assertEquals("DEFAULT.name", node.toString());
    }

    // a.age
    @Test
    public void variable_5_test() {
        RexNode node = builder.source(mockSourceConfig("a")).variable("a", "age");
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        Assert.assertEquals("a.age", node.toString());
    }

    // a.age + 10
    @Test
    public void plus_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode plus = builder.call(GraphStdOperatorTable.PLUS, var, builder.literal(10));
        Assert.assertEquals(plus.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        Assert.assertEquals("+(a.age, 10)", plus.toString());
    }

    // a.age == 10
    @Test
    public void equal_1_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode equal = builder.call(GraphStdOperatorTable.EQUALS, var, builder.literal(10));
        Assert.assertEquals(equal.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        Assert.assertEquals("=(a.age, 10)", equal.toString());
    }

    // a.age == 'X'
    // Integer is comparable with String in Calcite standard implementation
    // todo: maybe we need rewrite the implementation to add more constraints
    @Test
    public void equal_2_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode equal = builder.call(GraphStdOperatorTable.EQUALS, var, builder.literal("X"));
        Assert.assertEquals(equal.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        Assert.assertEquals("=(a.age, _UTF-8'X')", equal.toString());
    }

    // a.age + 10 == 30
    @Test
    public void equal_plus_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode plus = builder.call(GraphStdOperatorTable.PLUS, var, builder.literal(10));
        RexNode equal = builder.call(GraphStdOperatorTable.EQUALS, plus, builder.literal(30));
        Assert.assertEquals(equal.getType().getSqlTypeName(), SqlTypeName.BOOLEAN);
        Assert.assertEquals("=(+(a.age, 10), 30)", equal.toString());
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
        Assert.assertEquals("AND(>(a.age, 10), =(a.name, _UTF-8'x'))", node.toString());
    }

    // a.age % 10
    @Test
    public void mod_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode node = builder.call(GraphStdOperatorTable.MOD, var, builder.literal(10));
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        Assert.assertEquals("MOD(a.age, 10)", node.toString());
    }

    // a.age ^ 2
    @Test
    public void power_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode node = builder.call(GraphStdOperatorTable.POWER, var, builder.literal(2));
        // return type of power is double for the snd argument can be negative, i.e. 2^(-3)
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.DOUBLE);
        Assert.assertEquals("POWER(a.age, 2)", node.toString());
    }

    // -a.age
    @Test
    public void unary_minus_test() {
        RexNode var = builder.source(mockSourceConfig("a")).variable("a", "age");
        RexNode node = builder.call(GraphStdOperatorTable.UNARY_MINUS, var);
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.INTEGER);
        Assert.assertEquals("-(a.age)", node.toString());
    }

    // check the return type of the 'ARRAY_VALUE_CONSTRUCTOR', the return type inference strategy
    // is:
    // 1. derive the component type by finding the least restrictive type of the arguments
    // 2. if the least restrictive type cannot be found, use 'ANY' type
    // 3. derive the array type by the component type
    @Test
    public void array_value_constructor_test() {
        RexNode node =
                builder.call(
                        GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                        builder.literal(1),
                        builder.literal(2));
        Assert.assertEquals("INTEGER ARRAY", node.getType().toString());

        node =
                builder.call(
                        GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                        builder.literal(1),
                        builder.literal(2.0));
        Assert.assertEquals("DOUBLE ARRAY", node.getType().toString());

        node =
                builder.source(mockSourceConfig("a"))
                        .call(
                                GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                                builder.variable("a", "age"),
                                builder.literal(1));
        Assert.assertEquals("INTEGER ARRAY", node.getType().toString());

        node =
                builder.source(mockSourceConfig("a"))
                        .call(
                                GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                                builder.variable("a", "name"),
                                builder.literal(1));
        Assert.assertEquals("ANY ARRAY", node.getType().toString());
    }

    // check the return type of the 'MAP_VALUE_CONSTRUCTOR', the return type inference strategy is:
    // 1. derive the key or value type by finding the least restrictive type of the arguments
    // 2. if the least restrictive type cannot be found, use 'ANY' type
    // 3. derive the map type by the key or value type
    @Test
    public void map_value_constructor_test() {
        RexNode node =
                builder.source(mockSourceConfig("a"))
                        .call(
                                GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                                builder.literal("name"),
                                builder.variable("a", "name"),
                                builder.literal("age"),
                                builder.variable("a", "age"));
        Assert.assertEquals("(CHAR(4), ANY) MAP", node.getType().toString());
    }

    private SourceConfig mockSourceConfig(String alias) {
        return new SourceConfig(
                GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"), alias);
    }
}
