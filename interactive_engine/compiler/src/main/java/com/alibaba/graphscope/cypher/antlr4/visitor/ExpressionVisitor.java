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
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.cypher.antlr4.visitor.type.ExprVisitorResult;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExpressionVisitor extends CypherGSBaseVisitor<ExprVisitorResult> {
    private final GraphBuilderVisitor parent;
    private final GraphBuilder builder;
    private final ParamIdGenerator paramIdGenerator;
    // map paramId to param name
    private final ImmutableMap.Builder<Integer, String> paramsBuilder;

    public ExpressionVisitor(GraphBuilderVisitor parent) {
        this.parent = parent;
        this.builder = Objects.requireNonNull(parent).getGraphBuilder();
        this.paramIdGenerator = new ParamIdGenerator();
        this.paramsBuilder = ImmutableMap.builder();
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
        if (ObjectUtils.isEmpty(ctx.oC_NotExpression())) {
            throw new IllegalArgumentException("operands should not be empty in 'AND' operator");
        }
        return binaryCall(
                GraphStdOperatorTable.AND,
                ctx.oC_NotExpression().stream()
                        .map(k -> visitOC_NotExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_NotExpression(CypherGSParser.OC_NotExpressionContext ctx) {
        ExprVisitorResult operand = visitOC_ComparisonExpression(ctx.oC_ComparisonExpression());
        List<TerminalNode> notNodes = ctx.NOT();
        return unaryCall(
                ObjectUtils.isNotEmpty(notNodes) && (notNodes.size() & 1) != 0
                        ? ImmutableList.of(GraphStdOperatorTable.NOT)
                        : ImmutableList.of(),
                operand);
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
    public ExprVisitorResult visitOC_StringListNullPredicateExpression(
            CypherGSParser.OC_StringListNullPredicateExpressionContext ctx) {
        ExprVisitorResult operand =
                visitOC_AddOrSubtractExpression(ctx.oC_AddOrSubtractExpression());
        List<SqlOperator> operators = Lists.newArrayList();
        CypherGSParser.OC_NullPredicateExpressionContext nullCtx = ctx.oC_NullPredicateExpression();
        if (nullCtx != null) {
            if (nullCtx.IS() != null && nullCtx.NOT() != null && nullCtx.NULL() != null) {
                operators.add(GraphStdOperatorTable.IS_NOT_NULL);
            } else if (nullCtx.IS() != null && nullCtx.NULL() != null) {
                operators.add(GraphStdOperatorTable.IS_NULL);
            }
        }
        return unaryCall(operators, operand);
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
        return unaryCall(operators, operand);
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
    public ExprVisitorResult visitOC_PatternPredicate(
            CypherGSParser.OC_PatternPredicateContext ctx) {
        RelNode subQuery =
                parent.visitOC_RelationshipsPattern(ctx.oC_RelationshipsPattern()).build();
        return new ExprVisitorResult(RexSubQuery.exists(subQuery));
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
    public ExprVisitorResult visitOC_ListLiteral(CypherGSParser.OC_ListLiteralContext ctx) {
        List<ExprVisitorResult> operands =
                ctx.oC_Expression().stream()
                        .map(k -> visitOC_Expression(k))
                        .collect(Collectors.toList());
        List<RelBuilder.AggCall> aggCallList = Lists.newArrayList();
        List<RexNode> expressions = Lists.newArrayList();
        operands.forEach(
                k -> {
                    if (!k.getAggCalls().isEmpty()) {
                        aggCallList.addAll(k.getAggCalls());
                    }
                    expressions.add(k.getExpr());
                });
        return new ExprVisitorResult(
                aggCallList,
                builder.call(GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, expressions));
    }

    @Override
    public ExprVisitorResult visitOC_Parameter(CypherGSParser.OC_ParameterContext ctx) {
        String paramName = ctx.oC_SymbolicName().getText();
        int paramIndex = this.paramIdGenerator.generate(paramName);
        GraphRexBuilder rexBuilder = (GraphRexBuilder) builder.getRexBuilder();
        RexDynamicParam dynamicParam = rexBuilder.makeGraphDynamicParam(paramName, paramIndex);
        paramsBuilder.put(dynamicParam.getIndex(), paramName);
        return new ExprVisitorResult(dynamicParam);
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
        String functionName = ctx.oC_FunctionName().getText();
        switch (getFunctionType(functionName)) {
            case SIMPLE:
                return visitOC_SimpleFunction(ctx);
            case AGGREGATE:
                return visitOC_AggregateFunction(ctx);
            case USER_DEFINED:
            default:
                throw new UnsupportedOperationException(
                        "user defined function " + functionName + " is unsupported yet");
        }
    }

    public ExprVisitorResult visitOC_SimpleFunction(
            CypherGSParser.OC_FunctionInvocationContext ctx) {
        List<CypherGSParser.OC_ExpressionContext> exprCtx = ctx.oC_Expression();
        String functionName = ctx.oC_FunctionName().getText();
        switch (functionName.toUpperCase()) {
            case "LENGTH":
                Preconditions.checkArgument(
                        !exprCtx.isEmpty(), "LENGTH function should have one argument");
                return new ExprVisitorResult(
                        builder.variable(exprCtx.get(0).getText(), GraphProperty.LEN_KEY));
            case "HEAD":
                Preconditions.checkArgument(
                        !exprCtx.isEmpty(), "HEAD function should have one argument");
                String errorMessage =
                        "'head(collect(...))' is the only supported usage of HEAD function";
                ExprVisitorResult argResult = visitOC_Expression(exprCtx.get(0));
                List<RelBuilder.AggCall> aggCalls = argResult.getAggCalls();
                if (aggCalls.size() == 1) {
                    GraphAggCall oldAggCall = (GraphAggCall) aggCalls.get(0);
                    if (oldAggCall.getAggFunction().getKind() == SqlKind.COLLECT) {
                        // convert 'head(collect)' to 'first_value' aggregate function
                        RelBuilder.AggCall newAggCall =
                                new GraphAggCall(
                                                oldAggCall.getCluster(),
                                                GraphStdOperatorTable.FIRST_VALUE,
                                                oldAggCall.getOperands())
                                        .as(oldAggCall.getAlias());
                        return new ExprVisitorResult(
                                ImmutableList.of(newAggCall), argResult.getExpr());
                    } else {
                        throw new UnsupportedOperationException(
                                errorMessage
                                        + " , but got "
                                        + oldAggCall.getAggFunction().getName());
                    }
                } else {
                    throw new UnsupportedOperationException(errorMessage);
                }
            default:
                throw new IllegalArgumentException(
                        "simple function " + functionName + " is unsupported yet");
        }
    }

    private FunctionType getFunctionType(String functionName) {
        switch (functionName.toUpperCase()) {
            case "LENGTH":
            case "HEAD":
                return FunctionType.SIMPLE;
            case "COUNT":
            case "SUM":
            case "AVG":
            case "MIN":
            case "MAX":
            case "COLLECT":
                return FunctionType.AGGREGATE;
            default:
                return FunctionType.USER_DEFINED;
        }
    }

    private enum FunctionType {
        SIMPLE,
        AGGREGATE,
        USER_DEFINED
    }

    public ExprVisitorResult visitOC_AggregateFunction(
            CypherGSParser.OC_FunctionInvocationContext ctx) {
        List<RexNode> variables =
                ctx.oC_Expression().stream()
                        .map(k -> visitOC_Expression(k).getExpr())
                        .collect(Collectors.toList());
        RelBuilder.AggCall aggCall;
        String alias = parent.inferAlias();
        String functionName = ctx.oC_FunctionName().getText();
        boolean isDistinct = ctx.DISTINCT() != null;
        switch (functionName.toUpperCase()) {
            case "COUNT":
                aggCall = builder.count(isDistinct, alias, variables);
                break;
            case "SUM":
                aggCall = builder.sum(isDistinct, alias, variables.get(0));
                break;
            case "AVG":
                aggCall = builder.avg(isDistinct, alias, variables.get(0));
                break;
            case "MIN":
                aggCall = builder.min(alias, variables.get(0));
                break;
            case "MAX":
                aggCall = builder.max(alias, variables.get(0));
                break;
            case "COLLECT":
                aggCall = builder.collect((ctx.DISTINCT() != null), alias, variables);
                break;
            default:
                throw new UnsupportedOperationException(
                        "agg function " + functionName + " is unsupported yet");
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

    @Override
    public ExprVisitorResult visitOC_CaseExpression(CypherGSParser.OC_CaseExpressionContext ctx) {
        ExprVisitorResult inputExpr =
                ctx.oC_InputExpression() == null
                        ? null
                        : visitOC_InputExpression(ctx.oC_InputExpression());
        List<RexNode> operands = Lists.newArrayList();
        for (CypherGSParser.OC_CaseAlternativeContext whenThen : ctx.oC_CaseAlternative()) {
            Preconditions.checkArgument(
                    whenThen.oC_Expression().size() == 2,
                    "whenThen expression should have 2 parts");
            ExprVisitorResult whenExpr = visitOC_Expression(whenThen.oC_Expression(0));
            if (inputExpr != null) {
                operands.add(builder.equals(inputExpr.getExpr(), whenExpr.getExpr()));
            } else {
                operands.add(whenExpr.getExpr());
            }
            ExprVisitorResult thenExpr = visitOC_Expression(whenThen.oC_Expression(1));
            operands.add(thenExpr.getExpr());
        }
        // if else expression is omitted, the default value is null
        ExprVisitorResult elseExpr =
                ctx.oC_ElseExpression() == null
                        ? new ExprVisitorResult(builder.literal(null))
                        : visitOC_ElseExpression(ctx.oC_ElseExpression());
        operands.add(elseExpr.getExpr());
        return new ExprVisitorResult(builder.call(GraphStdOperatorTable.CASE, operands));
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

    /**
     *
     * @param operators at most one operator, can be empty
     * @param operand
     * @return
     */
    private ExprVisitorResult unaryCall(List<SqlOperator> operators, ExprVisitorResult operand) {
        return (operators.isEmpty())
                ? operand
                : new ExprVisitorResult(
                        operand.getAggCalls(), builder.call(operators.get(0), operand.getExpr()));
    }

    private class ParamIdGenerator {
        private final AtomicInteger idGenerator;
        private Map<String, Integer> paramNameToIdMap;

        public ParamIdGenerator() {
            this.idGenerator = new AtomicInteger();
            this.paramNameToIdMap = new HashMap<>();
        }

        public int generate(@Nullable String paramName) {
            Integer paramId = paramNameToIdMap.get(paramName);
            if (paramId == null) {
                paramId = idGenerator.getAndIncrement();
                paramNameToIdMap.put(paramName, paramId);
            }
            return paramId;
        }
    }

    public ImmutableMap<Integer, String> getDynamicParams() {
        return this.paramsBuilder.build();
    }
}
