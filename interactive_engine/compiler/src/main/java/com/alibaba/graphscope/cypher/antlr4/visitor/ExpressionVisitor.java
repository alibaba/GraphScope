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

import com.alibaba.graphscope.common.antlr4.ExprUniqueAliasInfer;
import com.alibaba.graphscope.common.antlr4.ExprVisitorResult;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.function.GraphFunctions;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexTmpVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExpressionVisitor extends CypherGSBaseVisitor<ExprVisitorResult> {
    private final GraphBuilderVisitor parent;
    private final ExprUniqueAliasInfer aliasInfer;
    private final GraphBuilder builder;
    // generate param id for each unique param name, and record the param id to param name mappings
    private final ParamManager paramManager;

    public ExpressionVisitor(GraphBuilderVisitor parent) {
        this.parent = Objects.requireNonNull(parent);
        this.aliasInfer = Objects.requireNonNull(parent.getAliasInfer());
        this.builder = parent.getGraphBuilder();
        this.paramManager = new ParamManager();
    }

    @Override
    public ExprVisitorResult visitOC_OrExpression(CypherGSParser.OC_OrExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_AndExpression())) {
            throw new IllegalArgumentException("and expression should not be empty");
        }
        return Utils.binaryCall(
                GraphStdOperatorTable.OR,
                ctx.oC_AndExpression().stream()
                        .map(k -> visitOC_AndExpression(k))
                        .collect(Collectors.toList()),
                builder);
    }

    @Override
    public ExprVisitorResult visitOC_AndExpression(CypherGSParser.OC_AndExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_NotExpression())) {
            throw new IllegalArgumentException("operands should not be empty in 'AND' operator");
        }
        return Utils.binaryCall(
                GraphStdOperatorTable.AND,
                ctx.oC_NotExpression().stream()
                        .map(k -> visitOC_NotExpression(k))
                        .collect(Collectors.toList()),
                builder);
    }

    @Override
    public ExprVisitorResult visitOC_NotExpression(CypherGSParser.OC_NotExpressionContext ctx) {
        ExprVisitorResult operand =
                visitOC_NullPredicateExpression(ctx.oC_NullPredicateExpression());
        List<TerminalNode> notNodes = ctx.NOT();
        return Utils.unaryCall(
                ObjectUtils.isNotEmpty(notNodes) && (notNodes.size() & 1) != 0
                        ? ImmutableList.of(GraphStdOperatorTable.NOT)
                        : ImmutableList.of(),
                operand,
                builder);
    }

    @Override
    public ExprVisitorResult visitOC_ComparisonExpression(
            CypherGSParser.OC_ComparisonExpressionContext ctx) {
        List<SqlOperator> operators = new ArrayList<>();
        List<ExprVisitorResult> operands = new ArrayList<>();
        operands.add(
                visitOC_StringOrListPredicateExpression(ctx.oC_StringOrListPredicateExpression()));
        for (CypherGSParser.OC_PartialComparisonExpressionContext partialCtx :
                ctx.oC_PartialComparisonExpression()) {
            operands.add(
                    visitOC_StringOrListPredicateExpression(
                            partialCtx.oC_StringOrListPredicateExpression()));
            operators.addAll(
                    Utils.getOperators(
                            partialCtx.children,
                            ImmutableList.of("=", "<>", "<", ">", "<=", ">="),
                            false));
        }
        return Utils.binaryCall(operators, operands, builder);
    }

    @Override
    public ExprVisitorResult visitOC_StringOrListPredicateExpression(
            CypherGSParser.OC_StringOrListPredicateExpressionContext ctx) {
        List<ExprVisitorResult> operands =
                Lists.newArrayList(
                        visitOC_AddOrSubtractOrBitManipulationExpression(
                                ctx.oC_AddOrSubtractOrBitManipulationExpression(0)));
        List<SqlOperator> operators = Lists.newArrayList();
        if (ctx.STARTS() != null && ctx.WITH() != null
                || ctx.ENDS() != null && ctx.WITH() != null
                || ctx.CONTAINS() != null) {
            RexNode rightExpr =
                    visitOC_AddOrSubtractOrBitManipulationExpression(
                                    ctx.oC_AddOrSubtractOrBitManipulationExpression(1))
                            .getExpr();
            // the right operand should be a string literal
            Preconditions.checkArgument(
                    rightExpr.getKind() == SqlKind.LITERAL
                            && rightExpr.getType().getFamily() == SqlTypeFamily.CHARACTER,
                    "the right operand of string predicate expression should be a string literal");
            String value = ((RexLiteral) rightExpr).getValueAs(NlsString.class).getValue();
            StringBuilder regexPattern = new StringBuilder();
            if (ctx.STARTS() != null && ctx.WITH() != null) {
                regexPattern.append("^");
                regexPattern.append(value);
                regexPattern.append(".*");
            } else if (ctx.ENDS() != null && ctx.WITH() != null) {
                regexPattern.append(".*");
                regexPattern.append(value);
                regexPattern.append("$");
            } else {
                regexPattern.append(".*");
                regexPattern.append(value);
                regexPattern.append(".*");
            }
            operands.add(new ExprVisitorResult(builder.literal(regexPattern.toString())));
            operators.add(GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE);
        } else if (ctx.IN() != null) {
            operands.add(
                    visitOC_AddOrSubtractOrBitManipulationExpression(
                            ctx.oC_AddOrSubtractOrBitManipulationExpression(1)));
            operators.add(GraphStdOperatorTable.IN);
        }
        return Utils.binaryCall(operators, operands, builder);
    }

    @Override
    public ExprVisitorResult visitOC_NullPredicateExpression(
            CypherGSParser.OC_NullPredicateExpressionContext ctx) {
        ExprVisitorResult operand = visitOC_ComparisonExpression(ctx.oC_ComparisonExpression());
        List<SqlOperator> operators = Lists.newArrayList();
        if (ctx.IS() != null && ctx.NOT() != null && ctx.NULL() != null) {
            operators.add(GraphStdOperatorTable.IS_NOT_NULL);
        } else if (ctx.IS() != null && ctx.NULL() != null) {
            operators.add(GraphStdOperatorTable.IS_NULL);
        }
        return Utils.unaryCall(operators, operand, builder);
    }

    @Override
    public ExprVisitorResult visitOC_AddOrSubtractOrBitManipulationExpression(
            CypherGSParser.OC_AddOrSubtractOrBitManipulationExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_MultiplyDivideModuloExpression())) {
            throw new IllegalArgumentException("multiply or divide expression should not be empty");
        }
        List<SqlOperator> operators =
                com.alibaba.graphscope.common.antlr4.Utils.getOperators(
                        ctx.children, ImmutableList.of("+", "-", "&", "|", "^", "<<", ">>"), false);
        return Utils.binaryCall(
                operators,
                ctx.oC_MultiplyDivideModuloExpression().stream()
                        .map(k -> visitOC_MultiplyDivideModuloExpression(k))
                        .collect(Collectors.toList()),
                builder);
    }

    @Override
    public ExprVisitorResult visitOC_MultiplyDivideModuloExpression(
            CypherGSParser.OC_MultiplyDivideModuloExpressionContext ctx) {
        if (ObjectUtils.isEmpty(ctx.oC_UnaryAddOrSubtractExpression())) {
            throw new IllegalArgumentException("power expression should not be empty");
        }
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("*", "/", "%"), false);
        return Utils.binaryCall(
                operators,
                ctx.oC_UnaryAddOrSubtractExpression().stream()
                        .map(k -> visitOC_UnaryAddOrSubtractExpression(k))
                        .collect(Collectors.toList()),
                builder);
    }

    @Override
    public ExprVisitorResult visitOC_UnaryAddOrSubtractExpression(
            CypherGSParser.OC_UnaryAddOrSubtractExpressionContext ctx) {
        ExprVisitorResult operand = visitOC_ListOperatorExpression(ctx.oC_ListOperatorExpression());
        List<SqlOperator> operators =
                Utils.getOperators(ctx.children, ImmutableList.of("-", "+"), true);
        return Utils.unaryCall(operators, operand, builder);
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
                String propertyName = ctx.oC_PropertyLookup().oC_PropertyKeyName().getText();
                ExprVisitorResult exprRes = visitOC_Atom(ctx.oC_Atom());
                RexNode expr = exprRes.getExpr();
                // get property from a vertex or an edge
                if (expr.getType() instanceof GraphSchemaType) {
                    Preconditions.checkArgument(
                            expr instanceof RexGraphVariable,
                            "can not get property from the rex=",
                            expr);
                    String aliasName = ((RexGraphVariable) expr).getName().split("\\.")[0];
                    return new ExprVisitorResult(
                            exprRes.getAggCalls(), builder.variable(aliasName, propertyName));
                }
                // get interval from a date time
                if (SqlTypeFamily.DATETIME
                        .getTypeNames()
                        .contains(expr.getType().getSqlTypeName())) {
                    return new ExprVisitorResult(
                            exprRes.getAggCalls(),
                            builder.call(
                                    GraphStdOperatorTable.EXTRACT,
                                    Utils.createIntervalExpr(
                                            null, Utils.createExtractUnit(propertyName), builder),
                                    expr));
                }
                throw new IllegalArgumentException(
                        "invalid property lookup operation, cannot get property or extract interval"
                                + " from expr=["
                                + expr
                                + ", type="
                                + expr.getType()
                                + "]");
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
    public ExprVisitorResult visitOC_MapLiteral(CypherGSParser.OC_MapLiteralContext ctx) {
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
    public ExprVisitorResult visitOC_Parameter(CypherGSParser.OC_ParameterContext ctx) {
        String paramName = ctx.oC_SymbolicName().getText();
        int paramIndex = this.paramManager.generate(paramName);
        GraphRexBuilder rexBuilder = (GraphRexBuilder) builder.getRexBuilder();
        RexDynamicParam dynamicParam = rexBuilder.makeGraphDynamicParam(paramName, paramIndex);
        paramManager.addIdToName(dynamicParam.getIndex(), paramName);
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
    public ExprVisitorResult visitOC_AggregateFunctionInvocation(
            CypherGSParser.OC_AggregateFunctionInvocationContext ctx) {
        List<RexNode> variables =
                ctx.oC_Expression().stream()
                        .map(k -> visitOC_Expression(k).getExpr())
                        .collect(Collectors.toList());
        RelBuilder.AggCall aggCall;
        String alias = aliasInfer.infer();
        String functionName = ctx.getChild(0).getText();
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
                        "aggregate function " + functionName + " is unsupported yet");
        }
        return new ExprVisitorResult(
                ImmutableList.of(aggCall),
                RexTmpVariable.of(alias, ((GraphAggCall) aggCall).getType()));
    }

    @Override
    public ExprVisitorResult visitOC_UserDefinedFunctionInvocation(
            CypherGSParser.OC_UserDefinedFunctionInvocationContext ctx) {
        String functionName = ctx.oC_UserDefinedFunctionName().getText();
        List<RelBuilder.AggCall> aggCalls = Lists.newArrayList();
        List<RexNode> parameters = Lists.newArrayList();
        ctx.oC_Expression()
                .forEach(
                        k -> {
                            ExprVisitorResult res = visitOC_Expression(k);
                            aggCalls.addAll(res.getAggCalls());
                            parameters.add(res.getExpr());
                        });
        Configs configs = parent.getGraphBuilder().getContext().unwrapOrThrow(Configs.class);
        GraphFunctions functions = GraphFunctions.instance(configs);
        RexNode udfCall =
                builder.call(
                        GraphStdOperatorTable.USER_DEFINED_FUNCTION(
                                functions.getFunction(functionName)),
                        parameters);
        return new ExprVisitorResult(aggCalls, udfCall);
    }

    @Override
    public ExprVisitorResult visitOC_ScalarFunctionInvocation(
            CypherGSParser.OC_ScalarFunctionInvocationContext ctx) {
        List<CypherGSParser.OC_ExpressionContext> exprCtx = ctx.oC_Expression();
        String functionName = ctx.getChild(0).getText();
        switch (functionName.toUpperCase()) {
            case "LABELS":
                RexNode labelVar = builder.variable(exprCtx.get(0).getText());
                Preconditions.checkArgument(
                        labelVar.getType() instanceof GraphSchemaType
                                && ((GraphSchemaType) labelVar.getType()).getScanOpt()
                                        == GraphOpt.Source.VERTEX,
                        "'labels' can only be applied on vertex type");
                return new ExprVisitorResult(
                        builder.variable(exprCtx.get(0).getText(), GraphProperty.LABEL_KEY));
            case "TYPE":
                RexNode typeVar = builder.variable(exprCtx.get(0).getText());
                Preconditions.checkArgument(
                        typeVar.getType() instanceof GraphSchemaType
                                && ((GraphSchemaType) typeVar.getType()).getScanOpt()
                                        == GraphOpt.Source.EDGE,
                        "'type' can only be applied on edge type");
                return new ExprVisitorResult(
                        builder.variable(exprCtx.get(0).getText(), GraphProperty.LABEL_KEY));
            case "ELEMENTID":
                RexNode idVar = builder.variable(exprCtx.get(0).getText());
                Preconditions.checkArgument(
                        idVar.getType() instanceof GraphSchemaType,
                        "'elementId' can only be applied on vertex or edge type");
                return new ExprVisitorResult(
                        builder.variable(exprCtx.get(0).getText(), GraphProperty.ID_KEY));
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
            case "DURATION":
                ExprVisitorResult operand = visitOC_Expression(exprCtx.get(0));
                Preconditions.checkArgument(
                        operand.getExpr().getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR,
                        "parameter of scalar function 'duration' should be a map literal");
                RexCall mapValues = (RexCall) operand.getExpr();
                RexNode intervals = null;
                for (int i = 0; i < mapValues.getOperands().size(); i += 2) {
                    RexNode key = mapValues.getOperands().get(i);
                    RexNode value = mapValues.getOperands().get(i + 1);
                    String timeField = ((RexLiteral) key).getValueAs(NlsString.class).getValue();
                    RexNode interval =
                            Utils.createIntervalExpr(
                                    value, Utils.createDurationUnit(timeField), builder);
                    intervals =
                            (intervals == null)
                                    ? interval
                                    : builder.call(GraphStdOperatorTable.PLUS, intervals, interval);
                }
                Preconditions.checkArgument(
                        intervals != null,
                        "parameter of scalar function 'duration' should not be empty");
                return new ExprVisitorResult(operand.getAggCalls(), intervals);
            default:
                throw new IllegalArgumentException(
                        "scalar function " + functionName + " is unsupported yet");
        }
    }

    @Override
    public ExprVisitorResult visitOC_CountAny(CypherGSParser.OC_CountAnyContext ctx) {
        String alias = aliasInfer.infer();
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

    private class ParamManager {
        private final AtomicInteger idGenerator;
        private Map<String, Integer> paramNameToIdMap;
        private Map<Integer, String> paramIdToNameMap;

        public ParamManager() {
            this.idGenerator = new AtomicInteger();
            this.paramNameToIdMap = Maps.newHashMap();
            this.paramIdToNameMap = Maps.newHashMap();
        }

        public int generate(@Nullable String paramName) {
            Integer paramId = paramNameToIdMap.get(paramName);
            if (paramId == null) {
                paramId = idGenerator.getAndIncrement();
                paramNameToIdMap.put(paramName, paramId);
            }
            return paramId;
        }

        public void addIdToName(int paramId, String paramName) {
            paramIdToNameMap.put(paramId, paramName);
        }
    }

    public Map<Integer, String> getDynamicParams() {
        return Collections.unmodifiableMap(paramManager.paramIdToNameMap);
    }
}
