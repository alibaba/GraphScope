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

package com.alibaba.graphscope.cypher.antlr4.visitor;

import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexTmpVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.cypher.antlr4.type.ExprVisitorResult;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

public class ExpressionVisitor extends CypherGSBaseVisitor<ExprVisitorResult> {
    private final GraphBuilderVisitor parent;
    private final GraphBuilder builder;

    public ExpressionVisitor(GraphBuilderVisitor parent) {
        this.parent = parent;
        this.builder = Objects.requireNonNull(parent).getGraphBuilder();
    }

    @Override
    public ExprVisitorResult visitOC_OrExpression(CypherGSParser.OC_OrExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_AndExpression())) {
            throw new IllegalArgumentException("and expression should not be empty");
        }
        return binaryCall(
                GraphStdOperatorTable.OR,
                ctx.oC_AndExpression().stream()
                        .map(k -> visitOC_AndExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_AndExpression(CypherGSParser.OC_AndExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_ComparisonExpression())) {
            throw new IllegalArgumentException("comparison expression should not be empty");
        }
        return binaryCall(
                GraphStdOperatorTable.AND,
                ctx.oC_ComparisonExpression().stream()
                        .map(k -> visitOC_ComparisonExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_ComparisonExpression(
            CypherGSParser.OC_ComparisonExpressionContext ctx) {
        List<SqlOperator> operators = new ArrayList<>();
        List<ExprVisitorResult> operands = new ArrayList<>();
        operands.add(
                visitOC_StringListNullPredicateExpression(
                        ctx.oC_StringListNullPredicateExpression()));
        for (CypherGSParser.OC_PartialComparisonExpressionContext partialCtx :
                ctx.oC_PartialComparisonExpression()) {
            operands.add(
                    visitOC_StringListNullPredicateExpression(
                            partialCtx.oC_StringListNullPredicateExpression()));
            operators.addAll(
                    Utils.getOperators(
                            partialCtx.children,
                            ImmutableList.of("=", "<>", "<", ">", "<=", ">="),
                            false));
        }
        return binaryCall(operators, operands);
    }

    @Override
    public ExprVisitorResult visitOC_AddOrSubtractExpression(
            CypherGSParser.OC_AddOrSubtractExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_MultiplyDivideModuloExpression())) {
            throw new IllegalArgumentException("multiply or divide expression should not be empty");
        }
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("+", "-"), false);
        return binaryCall(
                operators,
                ctx.oC_MultiplyDivideModuloExpression().stream()
                        .map(k -> visitOC_MultiplyDivideModuloExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_MultiplyDivideModuloExpression(
            CypherGSParser.OC_MultiplyDivideModuloExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_PowerOfExpression())) {
            throw new IllegalArgumentException("power expression should not be empty");
        }
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("*", "/", "%"), false);
        return binaryCall(
                operators,
                ctx.oC_PowerOfExpression().stream()
                        .map(k -> visitOC_PowerOfExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_PowerOfExpression(
            CypherGSParser.OC_PowerOfExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_UnaryAddOrSubtractExpression())) {
            throw new IllegalArgumentException(
                    "unary add or unary sub expression should not be empty");
        }
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("^"), false);
        return binaryCall(
                operators,
                ctx.oC_UnaryAddOrSubtractExpression().stream()
                        .map(k -> visitOC_UnaryAddOrSubtractExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_UnaryAddOrSubtractExpression(
            CypherGSParser.OC_UnaryAddOrSubtractExpressionContext ctx) {
        ExprVisitorResult operand = visitOC_ListOperatorExpression(ctx.oC_ListOperatorExpression());
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("-", "+"), true);
        return (operators.isEmpty()) ? operand : unaryCall(operators.get(0), operand);
    }

    @Override
    public ExprVisitorResult visitOC_PropertyOrLabelsExpression(
            CypherGSParser.OC_PropertyOrLabelsExpressionContext ctx) {
        if (ctx.oC_PropertyLookup() == null) {
            return visitOC_Atom(ctx.oC_Atom());
        } else {
            if (ctx.oC_Atom().oC_Literal() != null) {
                throw new IllegalArgumentException("cannot get property from an literal");
            } else {
                String aliasName = ctx.oC_Atom().oC_Variable().getText();
                String propertyName = ctx.oC_PropertyLookup().oC_PropertyKeyName().getText();
                return new ExprVisitorResult(builder.variable(aliasName, propertyName));
            }
        }
    }

    @Override
    public ExprVisitorResult visitOC_ParenthesizedExpression(
            CypherGSParser.OC_ParenthesizedExpressionContext ctx) {
        return visitOC_Expression(ctx.oC_Expression());
    }

    @Override
    public ExprVisitorResult visitOC_Variable(CypherGSParser.OC_VariableContext ctx) {
        String aliasName = ctx.getText();
        return new ExprVisitorResult(builder.variable(aliasName));
    }

    @Override
    public ExprVisitorResult visitOC_Literal(CypherGSParser.OC_LiteralContext ctx) {
        if (ctx.StringLiteral() != null) {
            return new ExprVisitorResult(
                    builder.literal(LiteralVisitor.INSTANCE.visitTerminal(ctx.StringLiteral())));
        } else if (ctx.NULL() != null) {
            return new ExprVisitorResult(
                    builder.literal(LiteralVisitor.INSTANCE.visitTerminal(ctx.NULL())));
        } else {
            return super.visitOC_Literal(ctx);
        }
    }

    @Override
    public ExprVisitorResult visitOC_BooleanLiteral(CypherGSParser.OC_BooleanLiteralContext ctx) {
        return new ExprVisitorResult(
                builder.literal(LiteralVisitor.INSTANCE.visitOC_BooleanLiteral(ctx)));
    }

    @Override
    public ExprVisitorResult visitOC_IntegerLiteral(CypherGSParser.OC_IntegerLiteralContext ctx) {
        return new ExprVisitorResult(
                builder.literal(LiteralVisitor.INSTANCE.visitOC_IntegerLiteral(ctx)));
    }

    @Override
    public ExprVisitorResult visitOC_DoubleLiteral(CypherGSParser.OC_DoubleLiteralContext ctx) {
        return new ExprVisitorResult(
                builder.literal(LiteralVisitor.INSTANCE.visitOC_DoubleLiteral(ctx)));
    }

    @Override
    public ExprVisitorResult visitOC_FunctionInvocation(
            CypherGSParser.OC_FunctionInvocationContext ctx) {
        List<RexNode> variables =
                ctx.oC_Expression().stream()
                        .map(k -> visitOC_Expression(k).getExpr())
                        .collect(Collectors.toList());
        RelBuilder.AggCall aggCall;
        String alias = parent.inferAlias();
        if (ctx.oC_FunctionName().COUNT() != null) {
            aggCall = builder.count((ctx.DISTINCT() != null), alias, variables);
        } else if (ctx.oC_FunctionName().SUM() != null) {
            aggCall = builder.sum((ctx.DISTINCT() != null), alias, variables.get(0));
        } else if (ctx.oC_FunctionName().AVG() != null) {
            aggCall = builder.avg((ctx.DISTINCT() != null), alias, variables.get(0));
        } else if (ctx.oC_FunctionName().MIN() != null) {
            aggCall = builder.min(alias, variables.get(0));
        } else if (ctx.oC_FunctionName().MAX() != null) {
            aggCall = builder.max(alias, variables.get(0));
        } else if (ctx.oC_FunctionName().COLLECT() != null) {
            aggCall = builder.collect((ctx.DISTINCT() != null), alias, variables);
        } else {
            throw new UnsupportedOperationException(
                    "agg function " + ctx.oC_FunctionName().getText() + " is unsupported yet");
        }
        return new ExprVisitorResult(
                ImmutableList.of(aggCall),
                RexTmpVariable.of(alias, ((GraphAggCall) aggCall).getType()));
    }

    @Override
    public ExprVisitorResult visitOC_CountAny(CypherGSParser.OC_CountAnyContext ctx) {
        String alias = parent.inferAlias();
        RelBuilder.AggCall aggCall = builder.count();
        return new ExprVisitorResult(
                ImmutableList.of(aggCall),
                RexTmpVariable.of(alias, ((GraphAggCall) aggCall).getType()));
    }

    private ExprVisitorResult binaryCall(
            List<SqlOperator> operators, List<ExprVisitorResult> operands) {
        ObjectUtils.requireNonEmpty(operands, "operands count should not be 0");
        if (operators.size() != operands.size() - 1) {
            throw new IllegalArgumentException(
                    "invalid operators count, should be equal with the count of operands minus 1");
        }
        RexNode expr = operands.get(0).getExpr();
        List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
        aggCalls.addAll(operands.get(0).getAggCalls());
        for (int i = 1; i < operands.size(); ++i) {
            expr = builder.call(operators.get(i - 1), expr, operands.get(i).getExpr());
            aggCalls.addAll(operands.get(i).getAggCalls());
        }
        return new ExprVisitorResult(aggCalls, expr);
    }

    private ExprVisitorResult binaryCall(SqlOperator operator, List<ExprVisitorResult> operands) {
        ObjectUtils.requireNonEmpty(operands, "operands count should not be 0");
        RexNode expr = operands.get(0).getExpr();
        List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
        aggCalls.addAll(operands.get(0).getAggCalls());
        for (int i = 1; i < operands.size(); ++i) {
            expr = builder.call(operator, expr, operands.get(i).getExpr());
            aggCalls.addAll(operands.get(i).getAggCalls());
        }
        return new ExprVisitorResult(aggCalls, expr);
    }

    private ExprVisitorResult unaryCall(SqlOperator operator, ExprVisitorResult operand) {
        return new ExprVisitorResult(
                operand.getAggCalls(), builder.call(operator, operand.getExpr()));
    }
}
