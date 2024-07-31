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

package com.alibaba.graphscope.common.antlr4;

import com.alibaba.graphscope.common.ir.rex.RexGraphDynamicParam;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    /**
     * create {@code SqlOperator}(s) according to the given parser trees and opSigns
     * @param trees candidate parser trees
     * @param opSigns for each parser tree which denotes an operator, if it's type is contained in opSigns, then create a {@code SqlOperator} for it
     * @param isPrefix to distinguish unary and binary operators, i.e. unary plus VS binary plus
     * @return
     */
    public static List<SqlOperator> getOperators(
            List<ParseTree> trees, List<String> opSigns, boolean isPrefix) {
        List<SqlOperator> operators = new ArrayList<>();
        for (ParseTree tree : trees) {
            if (tree instanceof TerminalNode && opSigns.contains(tree.getText())) {
                if (tree.getText().equals("+")) {
                    if (isPrefix) {
                        operators.add(GraphStdOperatorTable.UNARY_PLUS);
                    } else {
                        operators.add(GraphStdOperatorTable.PLUS);
                    }
                } else if (tree.getText().equals("-")) {
                    if (isPrefix) {
                        operators.add(GraphStdOperatorTable.UNARY_MINUS);
                    } else {
                        operators.add(GraphStdOperatorTable.MINUS);
                    }
                } else if (tree.getText().equals("*")) {
                    operators.add(GraphStdOperatorTable.MULTIPLY);
                } else if (tree.getText().equals("/")) {
                    operators.add(GraphStdOperatorTable.DIVIDE);
                } else if (tree.getText().equals("%")) {
                    operators.add(GraphStdOperatorTable.MOD);
                } else if (tree.getText().equals("=")) {
                    operators.add(GraphStdOperatorTable.EQUALS);
                } else if (tree.getText().equals("<>")) {
                    operators.add(GraphStdOperatorTable.NOT_EQUALS);
                } else if (tree.getText().equals("<")) {
                    operators.add(GraphStdOperatorTable.LESS_THAN);
                } else if (tree.getText().equals(">")) {
                    operators.add(GraphStdOperatorTable.GREATER_THAN);
                } else if (tree.getText().equals("<=")) {
                    operators.add(GraphStdOperatorTable.LESS_THAN_OR_EQUAL);
                } else if (tree.getText().equals(">=")) {
                    operators.add(GraphStdOperatorTable.GREATER_THAN_OR_EQUAL);
                } else if (tree.getText().equals("&")) {
                    operators.add(GraphStdOperatorTable.BIT_AND);
                } else if (tree.getText().equals("|")) {
                    operators.add(GraphStdOperatorTable.BIT_OR);
                } else if (tree.getText().equals("^")) {
                    operators.add(GraphStdOperatorTable.BIT_XOR);
                } else if (tree.getText().equals("<<")) {
                    operators.add(GraphStdOperatorTable.BIT_LEFT_SHIFT);
                } else if (tree.getText().equals(">>")) {
                    operators.add(GraphStdOperatorTable.BIT_RIGHT_SHIFT);
                } else {
                    throw new UnsupportedOperationException(
                            "operator " + tree.getText() + " is unsupported yet");
                }
            }
        }
        return operators;
    }

    public static TimeUnit createDurationUnit(String fieldName) {
        switch (fieldName.toUpperCase()) {
            case "YEARS":
                return TimeUnit.YEAR;
            case "QUARTERS":
                return TimeUnit.QUARTER;
            case "MONTHS":
                return TimeUnit.MONTH;
            case "WEEKS":
                return TimeUnit.WEEK;
            case "DAYS":
                return TimeUnit.DAY;
            case "HOURS":
                return TimeUnit.HOUR;
            case "MINUTES":
                return TimeUnit.MINUTE;
            case "SECONDS":
                return TimeUnit.SECOND;
            case "MILLISECONDS":
                return TimeUnit.MILLISECOND;
            case "MICROSECONDS":
                return TimeUnit.MICROSECOND;
            case "NANOSECONDS":
                return TimeUnit.NANOSECOND;
            default:
                throw new UnsupportedOperationException(
                        "duration field name " + fieldName + " is unsupported yet");
        }
    }

    public static RexNode createIntervalExpr(
            @Nullable RexNode value, TimeUnit unit, GraphBuilder builder) {
        SqlIntervalQualifier intervalQualifier =
                new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
        if (value == null) {
            return builder.getRexBuilder().makeIntervalLiteral(null, intervalQualifier);
        } else if (value instanceof RexLiteral) {
            return builder.getRexBuilder()
                    .makeIntervalLiteral(
                            new BigDecimal(
                                    ((RexLiteral) value).getValueAs(Number.class).toString()),
                            intervalQualifier);
        } else if (value instanceof RexGraphDynamicParam) {
            RexGraphDynamicParam param = (RexGraphDynamicParam) value;
            return ((GraphRexBuilder) builder.getRexBuilder())
                    .makeGraphDynamicParam(
                            builder.getTypeFactory().createSqlIntervalType(intervalQualifier),
                            param.getName(),
                            param.getIndex());
        }
        throw new IllegalArgumentException("cannot create interval expression from value " + value);
    }

    public static TimeUnit createExtractUnit(String fieldName) {
        return TimeUnit.valueOf(fieldName.toUpperCase());
    }

    public static ExprVisitorResult binaryCall(
            List<SqlOperator> operators, List<ExprVisitorResult> operands, GraphBuilder builder) {
        ObjectUtils.requireNonEmpty(operands, "operands count should not be 0");
        if (operators.size() != operands.size() - 1) {
            throw new IllegalArgumentException(
                    "invalid operators count, should be equal with the count of operands minus 1");
        }
        RexNode expr = operands.get(0).getExpr();
        List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
        aggCalls.addAll(operands.get(0).getAggCalls());
        for (int i = 1; i < operands.size(); ++i) {
            expr = binaryCall(expr, operands.get(i).getExpr(), operators.get(i - 1), builder);
            aggCalls.addAll(operands.get(i).getAggCalls());
        }
        return new ExprVisitorResult(aggCalls, expr);
    }

    public static ExprVisitorResult binaryCall(
            SqlOperator operator, List<ExprVisitorResult> operands, GraphBuilder builder) {
        ObjectUtils.requireNonEmpty(operands, "operands count should not be 0");
        RexNode expr = operands.get(0).getExpr();
        List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
        aggCalls.addAll(operands.get(0).getAggCalls());
        for (int i = 1; i < operands.size(); ++i) {
            expr = binaryCall(expr, operands.get(i).getExpr(), operator, builder);
            aggCalls.addAll(operands.get(i).getAggCalls());
        }
        return new ExprVisitorResult(aggCalls, expr);
    }

    private static RexNode binaryCall(
            RexNode left, RexNode right, SqlOperator operator, GraphBuilder builder) {
        if (operator.getKind() == SqlKind.MINUS
                && SqlTypeUtil.isOfSameTypeName(SqlTypeName.DATETIME_TYPES, left.getType())
                && SqlTypeUtil.isOfSameTypeName(SqlTypeName.DATETIME_TYPES, right.getType())) {
            return builder.call(
                    GraphStdOperatorTable.DATETIME_MINUS,
                    left,
                    right,
                    Utils.createIntervalExpr(null, TimeUnit.MILLISECOND, builder));
        }
        return builder.call(operator, left, right);
    }

    /**
     *
     * @param operators at most one operator, can be empty
     * @param operand
     * @return
     */
    public static ExprVisitorResult unaryCall(
            List<SqlOperator> operators, ExprVisitorResult operand, GraphBuilder builder) {
        return (operators.isEmpty())
                ? operand
                : new ExprVisitorResult(
                        operand.getAggCalls(), builder.call(operators.get(0), operand.getExpr()));
    }
}
