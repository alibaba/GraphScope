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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import com.alibaba.graphscope.common.antlr4.ExprUniqueAliasInfer;
import com.alibaba.graphscope.common.antlr4.ExprVisitorResult;
import com.alibaba.graphscope.common.antlr4.Utils;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexTmpVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Although we have uniformly defined expression syntax through file 'ExprGS', antlr only allows resource reuse rather than generated code. Therefore, we need to repeatedly reimplement related visitors for Gremlin to build the Calcite layer's expression structure from antlr trees.
 */
public class ExtExpressionVisitor extends GremlinGSBaseVisitor<ExprVisitorResult> {
    private final GraphBuilder builder;
    private final ExprUniqueAliasInfer aliasInfer;

    public ExtExpressionVisitor(GraphBuilder builder, ExprUniqueAliasInfer aliasInfer) {
        this.builder = Objects.requireNonNull(builder);
        this.aliasInfer = Objects.requireNonNull(aliasInfer);
    }

    @Override
    public ExprVisitorResult visitTraversalMethod_expr(
            GremlinGSParser.TraversalMethod_exprContext ctx) {
        return visitOC_Expression(ctx.oC_Expression());
    }

    @Override
    public ExprVisitorResult visitOC_OrExpression(GremlinGSParser.OC_OrExpressionContext ctx) {
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
    public ExprVisitorResult visitOC_AndExpression(GremlinGSParser.OC_AndExpressionContext ctx) {
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
    public ExprVisitorResult visitOC_NotExpression(GremlinGSParser.OC_NotExpressionContext ctx) {
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
            GremlinGSParser.OC_ComparisonExpressionContext ctx) {
        List<SqlOperator> operators = new ArrayList<>();
        List<ExprVisitorResult> operands = new ArrayList<>();
        operands.add(
                visitOC_StringListNullPredicateExpression(
                        ctx.oC_StringListNullPredicateExpression()));
        for (GremlinGSParser.OC_PartialComparisonExpressionContext partialCtx :
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
            GremlinGSParser.OC_StringListNullPredicateExpressionContext ctx) {
        ExprVisitorResult operand =
                visitOC_AddOrSubtractExpression(ctx.oC_AddOrSubtractExpression());
        Iterator i$ = ctx.children.iterator();
        while (i$.hasNext()) {
            ParseTree o = (ParseTree) i$.next();
            if (o == null) continue;
            if (GremlinGSParser.OC_NullPredicateExpressionContext.class.isInstance(o)) {
                operand =
                        visitOC_NullPredicateExpression(
                                operand, (GremlinGSParser.OC_NullPredicateExpressionContext) o);
            } else if (GremlinGSParser.OC_StringPredicateExpressionContext.class.isInstance(o)) {
                operand =
                        visitOC_StringPredicateExpression(
                                operand, (GremlinGSParser.OC_StringPredicateExpressionContext) o);
            }
        }
        return operand;
    }

    private ExprVisitorResult visitOC_NullPredicateExpression(
            ExprVisitorResult operand, GremlinGSParser.OC_NullPredicateExpressionContext nullCtx) {
        List<SqlOperator> operators = Lists.newArrayList();
        if (nullCtx.IS() != null && nullCtx.NOT() != null && nullCtx.NULL() != null) {
            operators.add(GraphStdOperatorTable.IS_NOT_NULL);
        } else if (nullCtx.IS() != null && nullCtx.NULL() != null) {
            operators.add(GraphStdOperatorTable.IS_NULL);
        } else {
            throw new IllegalArgumentException(
                    "unknown null predicate expression: " + nullCtx.getText());
        }
        return unaryCall(operators, operand);
    }

    private ExprVisitorResult visitOC_StringPredicateExpression(
            ExprVisitorResult operand,
            GremlinGSParser.OC_StringPredicateExpressionContext stringCtx) {
        ExprVisitorResult rightRes =
                visitOC_AddOrSubtractExpression(stringCtx.oC_AddOrSubtractExpression());
        RexNode rightExpr = rightRes.getExpr();
        // the right operand should be a string literal
        Preconditions.checkArgument(
                rightExpr.getKind() == SqlKind.LITERAL
                        && rightExpr.getType().getFamily() == SqlTypeFamily.CHARACTER,
                "the right operand of string predicate expression should be a string literal");
        String value = ((RexLiteral) rightExpr).getValueAs(NlsString.class).getValue();
        StringBuilder regexPattern = new StringBuilder();
        if (stringCtx.STARTS() != null) {
            regexPattern.append(value);
            regexPattern.append(".*");
        } else if (stringCtx.ENDS() != null) {
            regexPattern.append(".*");
            regexPattern.append(value);
        } else if (stringCtx.CONTAINS() != null) {
            regexPattern.append(".*");
            regexPattern.append(value);
            regexPattern.append(".*");
        } else {
            throw new IllegalArgumentException(
                    "unknown string predicate expression: " + stringCtx.getText());
        }
        return binaryCall(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                ImmutableList.of(
                        operand,
                        new ExprVisitorResult(
                                rightRes.getAggCalls(), builder.literal(regexPattern.toString()))));
    }

    @Override
    public ExprVisitorResult visitOC_AddOrSubtractExpression(
            GremlinGSParser.OC_AddOrSubtractExpressionContext ctx) {
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
            GremlinGSParser.OC_MultiplyDivideModuloExpressionContext ctx) {
        Preconditions.checkArgument(
                ObjectUtils.isNotEmpty(ctx.oC_BitManipulationExpression()),
                "bit manipulation expression should not be empty");
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("*", "/", "%"), false);
        return binaryCall(
                operators,
                ctx.oC_BitManipulationExpression().stream()
                        .map(k -> visitOC_BitManipulationExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_BitManipulationExpression(
            GremlinGSParser.OC_BitManipulationExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_UnaryAddOrSubtractExpression())) {
            throw new IllegalArgumentException(
                    "unary add or unary sub expression should not be empty");
        }
        List<SqlOperator> operators =
                Utils.getOperators(
                        ctx.children, ImmutableList.of("&", "|", "^", "<<", ">>"), false);
        return binaryCall(
                operators,
                ctx.oC_UnaryAddOrSubtractExpression().stream()
                        .map(k -> visitOC_UnaryAddOrSubtractExpression(k))
                        .collect(Collectors.toList()));
    }

    @Override
    public ExprVisitorResult visitOC_UnaryAddOrSubtractExpression(
            GremlinGSParser.OC_UnaryAddOrSubtractExpressionContext ctx) {
        ExprVisitorResult operand = visitOC_ListOperatorExpression(ctx.oC_ListOperatorExpression());
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("-", "+"), true);
        return unaryCall(operators, operand);
    }

    @Override
    public ExprVisitorResult visitOC_PropertyOrLabelsExpression(
            GremlinGSParser.OC_PropertyOrLabelsExpressionContext ctx) {
        if (ctx.oC_PropertyLookup() == null) {
            return visitOC_Atom(ctx.oC_Atom());
        } else {
            if (ctx.oC_Atom().oC_Literal() != null) {
                throw new IllegalArgumentException("cannot get property from an literal");
            } else {
                String aliasName = ctx.oC_Atom().oC_Variable().getText();
                RexGraphVariable variable = builder.variable(aliasName);
                String propertyName = ctx.oC_PropertyLookup().oC_PropertyKeyName().getText();
                RexNode expr =
                        (variable.getType() instanceof GraphSchemaType)
                                ? builder.variable(aliasName, propertyName)
                                : builder.call(
                                        GraphStdOperatorTable.EXTRACT,
                                        createIntervalLiteral(propertyName),
                                        variable);
                return new ExprVisitorResult(expr);
            }
        }
    }

    @Override
    public ExprVisitorResult visitOC_ParenthesizedExpression(
            GremlinGSParser.OC_ParenthesizedExpressionContext ctx) {
        return visitOC_Expression(ctx.oC_Expression());
    }

    @Override
    public ExprVisitorResult visitOC_Variable(GremlinGSParser.OC_VariableContext ctx) {
        String aliasName = ctx.getText();
        return new ExprVisitorResult(builder.variable(aliasName));
    }

    @Override
    public ExprVisitorResult visitOC_PatternPredicate(
            GremlinGSParser.OC_PatternPredicateContext ctx) {
        throw new UnsupportedOperationException(
                "pattern predicate is unsupported in gremlin expression");
    }

    @Override
    public ExprVisitorResult visitOC_Literal(GremlinGSParser.OC_LiteralContext ctx) {
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
    public ExprVisitorResult visitOC_ListLiteral(GremlinGSParser.OC_ListLiteralContext ctx) {
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
    public ExprVisitorResult visitOC_MapLiteral(GremlinGSParser.OC_MapLiteralContext ctx) {
        List<String> keys =
                ctx.oC_PropertyKeyName().stream()
                        .map(k -> k.getText())
                        .collect(Collectors.toList());
        List<ExprVisitorResult> values =
                ctx.oC_Expression().stream()
                        .map(k -> visitOC_Expression(k))
                        .collect(Collectors.toList());
        Preconditions.checkArgument(
                keys.size() == values.size(),
                "keys size="
                        + keys.size()
                        + " is not consistent with values size="
                        + values.size()
                        + " in MapLiteral");
        List<RelBuilder.AggCall> aggCallList = Lists.newArrayList();
        List<RexNode> expressions = Lists.newArrayList();
        for (int i = 0; i < keys.size(); ++i) {
            ExprVisitorResult valueExpr = values.get(i);
            if (!valueExpr.getAggCalls().isEmpty()) {
                aggCallList.addAll(valueExpr.getAggCalls());
            }
            expressions.add(builder.literal(keys.get(i)));
            expressions.add(valueExpr.getExpr());
        }
        return new ExprVisitorResult(
                aggCallList,
                builder.call(GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR, expressions));
    }

    @Override
    public ExprVisitorResult visitOC_BooleanLiteral(GremlinGSParser.OC_BooleanLiteralContext ctx) {
        return new ExprVisitorResult(
                builder.literal(LiteralVisitor.INSTANCE.visitOC_BooleanLiteral(ctx)));
    }

    @Override
    public ExprVisitorResult visitOC_IntegerLiteral(GremlinGSParser.OC_IntegerLiteralContext ctx) {
        return new ExprVisitorResult(
                builder.literal(LiteralVisitor.INSTANCE.visitOC_IntegerLiteral(ctx)));
    }

    @Override
    public ExprVisitorResult visitOC_DoubleLiteral(GremlinGSParser.OC_DoubleLiteralContext ctx) {
        return new ExprVisitorResult(
                builder.literal(LiteralVisitor.INSTANCE.visitOC_DoubleLiteral(ctx)));
    }

    @Override
    public ExprVisitorResult visitOC_FunctionInvocation(
            GremlinGSParser.OC_FunctionInvocationContext ctx) {
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
            GremlinGSParser.OC_FunctionInvocationContext ctx) {
        List<GremlinGSParser.OC_ExpressionContext> exprCtx = ctx.oC_Expression();
        String functionName = ctx.oC_FunctionName().getText();
        switch (functionName.toUpperCase()) {
            case "LABEL":
                return new ExprVisitorResult(
                        builder.variable(exprCtx.get(0).getText(), GraphProperty.LABEL_KEY));
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
            case "POWER":
                Preconditions.checkArgument(
                        exprCtx.size() == 2, "POWER function should have two arguments");
                ExprVisitorResult left = visitOC_Expression(exprCtx.get(0));
                ExprVisitorResult right = visitOC_Expression(exprCtx.get(1));
                List<RelBuilder.AggCall> allAggCalls = Lists.newArrayList();
                allAggCalls.addAll(left.getAggCalls());
                allAggCalls.addAll(right.getAggCalls());
                return new ExprVisitorResult(
                        allAggCalls,
                        builder.call(GraphStdOperatorTable.POWER, left.getExpr(), right.getExpr()));
            default:
                throw new IllegalArgumentException(
                        "simple function " + functionName + " is unsupported yet");
        }
    }

    private FunctionType getFunctionType(String functionName) {
        switch (functionName.toUpperCase()) {
            case "LABEL": // same as the denotation of 'label' step in gremlin
            case "LENGTH":
            case "HEAD":
            case "POWER":
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
            GremlinGSParser.OC_FunctionInvocationContext ctx) {
        List<RexNode> variables =
                ctx.oC_Expression().stream()
                        .map(k -> visitOC_Expression(k).getExpr())
                        .collect(Collectors.toList());
        RelBuilder.AggCall aggCall;
        String alias = aliasInfer.infer();
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
    public ExprVisitorResult visitOC_CountAny(GremlinGSParser.OC_CountAnyContext ctx) {
        String alias = aliasInfer.infer();
        RelBuilder.AggCall aggCall = builder.count();
        return new ExprVisitorResult(
                ImmutableList.of(aggCall),
                RexTmpVariable.of(alias, ((GraphAggCall) aggCall).getType()));
    }

    @Override
    public ExprVisitorResult visitOC_CaseExpression(GremlinGSParser.OC_CaseExpressionContext ctx) {
        ExprVisitorResult inputExpr =
                ctx.oC_InputExpression() == null
                        ? null
                        : visitOC_InputExpression(ctx.oC_InputExpression());
        List<RexNode> operands = Lists.newArrayList();
        for (GremlinGSParser.OC_CaseAlternativeContext whenThen : ctx.oC_CaseAlternative()) {
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

    private RexLiteral createIntervalLiteral(String fieldName) {
        TimeUnit timeUnit = TimeUnit.valueOf(fieldName.toUpperCase());
        SqlIntervalQualifier intervalQualifier =
                new SqlIntervalQualifier(timeUnit, null, SqlParserPos.ZERO);
        return builder.getRexBuilder().makeIntervalLiteral(null, intervalQualifier);
    }
}
