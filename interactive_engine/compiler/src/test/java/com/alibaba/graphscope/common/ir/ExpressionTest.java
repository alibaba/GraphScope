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

import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

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
        Assert.assertEquals("_", node.toString());
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
        } catch (FrontendException e) {
            return;
        }
        Assert.fail("tag 'a' does not exist");
    }

    // head.name
    @Test
    public void variable_4_test() {
        RexNode node = builder.source(mockSourceConfig(null)).variable(null, "name");
        Assert.assertEquals(node.getType().getSqlTypeName(), SqlTypeName.CHAR);
        Assert.assertEquals("_.name", node.toString());
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

    // @age + 1
    @Test
    public void dynamic_param_type_test() {
        GraphRexBuilder rexBuilder = (GraphRexBuilder) builder.getRexBuilder();
        RexNode plus =
                builder.call(
                        GraphStdOperatorTable.PLUS,
                        rexBuilder.makeGraphDynamicParam("age", 0),
                        builder.literal(1));
        Assert.assertEquals(SqlTypeName.INTEGER, plus.getType().getSqlTypeName());
    }

    @Test
    public void posix_regex_test() {
        RexNode regex =
                builder.source(mockSourceConfig(null))
                        .call(
                                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                                builder.variable(null, "name"),
                                builder.literal("^marko"));
        Assert.assertEquals(SqlTypeName.BOOLEAN, regex.getType().getSqlTypeName());
        Assert.assertEquals("POSIX REGEX CASE SENSITIVE(_.name, _UTF-8'^marko')", regex.toString());
    }

    @Test
    public void map_constructor_test() {
        RexNode map =
                builder.source(mockSourceConfig(null))
                        .call(
                                GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                                builder.literal("id"),
                                builder.variable(null, "id"),
                                builder.literal("age"),
                                builder.variable(null, "age"));
        Assert.assertEquals("MAP(_UTF-8'id', _.id, _UTF-8'age', _.age)", map.toString());
        // key type is string while value type is bigint
        Assert.assertEquals("(CHAR(3), BIGINT) MAP", map.getType().toString());
    }

    // extract year from creationDate, i.e. creationDate.day
    @Test
    public void extract_year_test() throws Exception {
        RexBuilder rexBuilder = builder.getRexBuilder();
        RexNode expr =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("software"),
                                        null))
                        .call(
                                GraphStdOperatorTable.EXTRACT,
                                rexBuilder.makeIntervalLiteral(
                                        null,
                                        new SqlIntervalQualifier(
                                                TimeUnit.DAY, null, SqlParserPos.ZERO)),
                                builder.variable(null, "creationDate"));
        Assert.assertEquals("EXTRACT(FLAG(DAY), _.creationDate)", expr.toString());
        RexToProtoConverter converter = new RexToProtoConverter(true, false, rexBuilder);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/extract_year.json").trim(),
                JsonFormat.printer().print(expr.accept(converter)));
    }

    // _.creationDate + duration({years: 3, months: 1})
    @Test
    public void date_plus_literal_interval_test() throws Exception {
        RexBuilder rexBuilder = builder.getRexBuilder();
        // interval + interval: 3 years + 1 month
        RexNode expr =
                builder.call(
                        GraphStdOperatorTable.PLUS,
                        rexBuilder.makeIntervalLiteral(
                                new BigDecimal("3"),
                                new SqlIntervalQualifier(TimeUnit.YEAR, null, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new BigDecimal("1"),
                                new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO)));
        Assert.assertEquals("+(3:INTERVAL YEAR, 1:INTERVAL MONTH)", expr.toString());
        // datetime + interval: creationDate + ( 3 years + 1 month )
        expr =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("software"),
                                        null))
                        .call(
                                GraphStdOperatorTable.PLUS,
                                builder.variable(null, "creationDate"),
                                expr);
        Assert.assertEquals(
                "+(_.creationDate, +(3:INTERVAL YEAR, 1:INTERVAL MONTH))", expr.toString());
        RexToProtoConverter converter = new RexToProtoConverter(true, false, rexBuilder);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/date_plus_literal_interval.json").trim(),
                JsonFormat.printer().print(expr.accept(converter)));
    }

    // _.creationDate + duration({years: $year, months: $month})
    @Test
    public void date_plus_param_interval_test() throws Exception {
        GraphRexBuilder rexBuilder = (GraphRexBuilder) builder.getRexBuilder();
        RexNode expr =
                builder.call(
                        GraphStdOperatorTable.PLUS,
                        rexBuilder.makeGraphDynamicParam(
                                builder.getTypeFactory()
                                        .createSqlIntervalType(
                                                new SqlIntervalQualifier(
                                                        TimeUnit.DAY, null, SqlParserPos.ZERO)),
                                "year",
                                0),
                        rexBuilder.makeGraphDynamicParam(
                                builder.getTypeFactory()
                                        .createSqlIntervalType(
                                                new SqlIntervalQualifier(
                                                        TimeUnit.HOUR, null, SqlParserPos.ZERO)),
                                "month",
                                1));
        expr =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("software"),
                                        null))
                        .call(
                                GraphStdOperatorTable.PLUS,
                                builder.variable(null, "creationDate"),
                                expr);
        Assert.assertEquals("+(_.creationDate, +(?0, ?1))", expr.toString());
        RexToProtoConverter converter = new RexToProtoConverter(true, false, rexBuilder);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/date_plus_param_interval.json").trim(),
                JsonFormat.printer().print(expr.accept(converter)));
    }

    // (a.creationDate - b.creationDate) / 1000 / 60
    @Test
    public void date_minus_date_test() throws Exception {
        RexBuilder rexBuilder = builder.getRexBuilder();
        RexNode expr =
                builder.source(new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(true), "a"))
                        .expand(new ExpandConfig(GraphOpt.Expand.BOTH))
                        .getV(new GetVConfig(GraphOpt.GetV.OTHER, new LabelConfig(true), "b"))
                        .call(
                                GraphStdOperatorTable.DATETIME_MINUS,
                                builder.variable("a", "creationDate"),
                                builder.variable("b", "creationDate"),
                                rexBuilder.makeIntervalLiteral(
                                        null,
                                        new SqlIntervalQualifier(
                                                TimeUnit.MILLISECOND, null, SqlParserPos.ZERO)));
        expr = builder.call(GraphStdOperatorTable.DIVIDE, expr, builder.literal(1000));
        Assert.assertEquals(
                "/(DATETIME_MINUS(a.creationDate, b.creationDate, null:INTERVAL MILLISECOND),"
                        + " 1000)",
                expr.toString());
        RexToProtoConverter converter = new RexToProtoConverter(true, false, rexBuilder);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/date_minus_date.json"),
                JsonFormat.printer().print(expr.accept(converter)));
    }

    private SourceConfig mockSourceConfig(String alias) {
        return new SourceConfig(
                GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person"), alias);
    }
}
