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

import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4.TraversalEnumParser;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.tinkerpop.gremlin.structure.T;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExpressionVisitor extends GremlinGSBaseVisitor<RexNode> {
    protected final RexNode propertyKey;
    protected final GraphBuilder builder;

    public static final int SIMPLE_PREDICATE_CHILD_COUNT = 1, CONNECTIVE_PREDICATE_CHILD_COUNT = 6;

    public ExpressionVisitor(GraphBuilder builder, RexNode propertyKey) {
        this.builder = Objects.requireNonNull(builder);
        this.propertyKey = Objects.requireNonNull(propertyKey);
    }

    @Override
    public RexNode visitTraversalMethod_valueMap(
            GremlinGSParser.TraversalMethod_valueMapContext ctx) {
        if (propertyKey.getType() instanceof GraphPathType) {
            return (new PathFunctionVisitor(builder, propertyKey))
                    .visitTraversalMethod_valueMap(ctx);
        }
        return builder.call(
                GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                getProperties(ctx, null).stream()
                        .flatMap(k -> Stream.of(builder.literal(k), builder.variable(null, k)))
                        .collect(Collectors.toList()));
    }

    @Override
    public RexNode visitTraversalMethod_values(GremlinGSParser.TraversalMethod_valuesContext ctx) {
        if (propertyKey.getType() instanceof GraphPathType) {
            return (new PathFunctionVisitor(builder, propertyKey)).visitTraversalMethod_values(ctx);
        }
        return builder.variable(null, (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
    }

    @Override
    public RexNode visitTraversalMethod_elementMap(
            GremlinGSParser.TraversalMethod_elementMapContext ctx) {
        if (propertyKey.getType() instanceof GraphPathType) {
            return (new PathFunctionVisitor(builder, propertyKey))
                    .visitTraversalMethod_elementMap(ctx);
        }
        return builder.call(
                GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                getProperties(ctx, null).stream()
                        .flatMap(k -> Stream.of(builder.literal(k), builder.variable(null, k)))
                        .collect(Collectors.toList()));
    }

    @Override
    public RexNode visitTraversalMethod_selectby(
            GremlinGSParser.TraversalMethod_selectbyContext byCtx) {
        Preconditions.checkArgument(
                propertyKey instanceof RexGraphVariable
                        && ((RexGraphVariable) propertyKey).getProperty() == null,
                "variable ["
                        + propertyKey
                        + "] cannot denote a start tag of the select by context");
        String tag = ((RexGraphVariable) propertyKey).getName();
        int byChildCount = byCtx.getChildCount();
        if (byChildCount == 3) { // select(..).by()
            return builder.variable(tag);
        }
        if (propertyKey.getType() instanceof GraphPathType) {
            return (new PathFunctionVisitor(builder, propertyKey))
                    .visitTraversalMethod_selectby(byCtx);
        }
        if (byChildCount == 4 && byCtx.StringLiteral() != null) { // select(..).by('name')
            return builder.variable(
                    tag, (String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral()));
        } else if (byChildCount == 4
                && byCtx.traversalToken() != null) { // select(..).by(T.label/T.id)
            T token =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            T.class, byCtx.traversalToken());
            return builder.variable(tag, token.getAccessor());
        } else if (byCtx.traversalMethod_valueMap() != null) { // select(..).by(valueMap('name'))
            return builder.call(
                    GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    getProperties(byCtx.traversalMethod_valueMap(), tag).stream()
                            .flatMap(k -> Stream.of(builder.literal(k), builder.variable(tag, k)))
                            .collect(Collectors.toList()));
        } else if (byCtx.traversalMethod_elementMap()
                != null) { // select(..).by(elementMap('name'))
            return builder.call(
                    GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    getProperties(byCtx.traversalMethod_elementMap(), tag).stream()
                            .flatMap(k -> Stream.of(builder.literal(k), builder.variable(tag, k)))
                            .collect(Collectors.toList()));
        } else if (byCtx.nestedTraversal() != null) {
            return Utils.convertExprToPair(
                            (new NestedTraversalRexVisitor(this.builder, tag, byCtx))
                                    .visitNestedTraversal(byCtx.nestedTraversal()))
                    .getValue0();
        }
        throw new UnsupportedEvalException(
                GremlinGSParser.TraversalMethod_selectbyContext.class,
                byCtx.getText() + " is unsupported yet in select");
    }

    private List<String> getProperties(
            GremlinGSParser.TraversalMethod_valueMapContext ctx, @Nullable String tag) {
        List<String> properties =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        return properties.isEmpty() ? getAllProperties(tag) : properties;
    }

    private List<String> getProperties(
            GremlinGSParser.TraversalMethod_elementMapContext ctx, @Nullable String tag) {
        List<String> properties =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (properties.isEmpty()) {
            properties.addAll(getAllProperties(tag));
        }
        properties.add(0, GraphProperty.LABEL_KEY);
        properties.add(1, GraphProperty.ID_KEY);
        return properties;
    }

    private List<String> getAllProperties(@Nullable String tag) {
        RexGraphVariable curVar = builder.variable(tag);
        RelDataType dataType = curVar.getType();
        Preconditions.checkArgument(
                dataType instanceof GraphSchemaType, "can not get property from type=", dataType);
        return dataType.getFieldList().stream().map(k -> k.getName()).collect(Collectors.toList());
    }

    @Override
    public RexNode visitTraversalPredicate(GremlinGSParser.TraversalPredicateContext ctx) {
        switch (ctx.getChildCount()) {
            case SIMPLE_PREDICATE_CHILD_COUNT:
                // handle simple predicate
                return visitChildren(ctx);
            case CONNECTIVE_PREDICATE_CHILD_COUNT: // simple predicates are connected by and/or
                final int childIndexOfParameterOperator = 2;
                final int childIndexOfCaller = 0;
                final int childIndexOfArgument = 4;

                if (ctx.getChild(childIndexOfParameterOperator).getText().equals("or")) {
                    // handle or
                    return builder.call(
                            GraphStdOperatorTable.OR,
                            visit(ctx.getChild(childIndexOfCaller)),
                            visit(ctx.getChild(childIndexOfArgument)));
                } else {
                    // handle and
                    return builder.call(
                            GraphStdOperatorTable.AND,
                            visit(ctx.getChild(childIndexOfCaller)),
                            visit(ctx.getChild(childIndexOfArgument)));
                }
            default:
                throw new UnsupportedEvalException(
                        ctx.getClass(),
                        "unexpected number of children in TraversalPredicateContext "
                                + ctx.getChildCount());
        }
    }

    @Override
    public RexNode visitTraversalPredicate_eq(GremlinGSParser.TraversalPredicate_eqContext ctx) {
        return builder.call(
                GraphStdOperatorTable.EQUALS,
                propertyKey,
                builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal())));
    }

    @Override
    public RexNode visitTraversalPredicate_neq(GremlinGSParser.TraversalPredicate_neqContext ctx) {
        return builder.call(
                GraphStdOperatorTable.NOT_EQUALS,
                propertyKey,
                builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal())));
    }

    @Override
    public RexNode visitTraversalPredicate_lt(GremlinGSParser.TraversalPredicate_ltContext ctx) {
        return builder.call(
                GraphStdOperatorTable.LESS_THAN,
                propertyKey,
                builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal())));
    }

    @Override
    public RexNode visitTraversalPredicate_lte(GremlinGSParser.TraversalPredicate_lteContext ctx) {
        return builder.call(
                GraphStdOperatorTable.LESS_THAN_OR_EQUAL,
                propertyKey,
                builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal())));
    }

    @Override
    public RexNode visitTraversalPredicate_gt(GremlinGSParser.TraversalPredicate_gtContext ctx) {
        return builder.call(
                GraphStdOperatorTable.GREATER_THAN,
                propertyKey,
                builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal())));
    }

    @Override
    public RexNode visitTraversalPredicate_gte(GremlinGSParser.TraversalPredicate_gteContext ctx) {
        return builder.call(
                GraphStdOperatorTable.GREATER_THAN_OR_EQUAL,
                propertyKey,
                builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal())));
    }

    @Override
    public RexNode visitTraversalPredicate_within(
            GremlinGSParser.TraversalPredicate_withinContext ctx) {
        List<Object> points =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Object.class);
        return builder.getRexBuilder()
                .makeIn(
                        propertyKey,
                        points.stream().map(k -> builder.literal(k)).collect(Collectors.toList()));
    }

    @Override
    public RexNode visitTraversalPredicate_without(
            GremlinGSParser.TraversalPredicate_withoutContext ctx) {
        List<Object> points =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Object.class);
        return builder.not(
                builder.getRexBuilder()
                        .makeIn(
                                propertyKey,
                                points.stream()
                                        .map(k -> builder.literal(k))
                                        .collect(Collectors.toList())));
    }

    @Override
    public RexNode visitTraversalPredicate_not(GremlinGSParser.TraversalPredicate_notContext ctx) {
        return builder.not(visitTraversalPredicate(ctx.traversalPredicate()));
    }

    @Override
    public RexNode visitTraversalPredicate_inside(
            GremlinGSParser.TraversalPredicate_insideContext ctx) {
        Number lower = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_Literal(0));
        Number upper = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_Literal(1));
        return builder.getRexBuilder()
                .makeBetween(
                        propertyKey,
                        builder.literal(new BigDecimal(lower.longValue() + 1)),
                        builder.literal(new BigDecimal(upper.longValue() - 1)));
    }

    @Override
    public RexNode visitTraversalPredicate_outside(
            GremlinGSParser.TraversalPredicate_outsideContext ctx) {
        Number lower = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_Literal(0));
        Number upper = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_Literal(1));
        return builder.not(
                builder.getRexBuilder()
                        .makeBetween(
                                propertyKey,
                                builder.literal(new BigDecimal(lower.longValue() + 1)),
                                builder.literal(new BigDecimal(upper.longValue() - 1))));
    }

    @Override
    public RexNode visitTraversalPredicate_startingWith(
            GremlinGSParser.TraversalPredicate_startingWithContext ctx) {
        String posixRegex = "^" + LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()) + ".*";
        return builder.call(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                propertyKey,
                builder.literal(posixRegex));
    }

    @Override
    public RexNode visitTraversalPredicate_notStartingWith(
            GremlinGSParser.TraversalPredicate_notStartingWithContext ctx) {
        String posixRegex = "^" + LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()) + ".*";
        return builder.not(
                builder.call(
                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                        propertyKey,
                        builder.literal(posixRegex)));
    }

    @Override
    public RexNode visitTraversalPredicate_endingWith(
            GremlinGSParser.TraversalPredicate_endingWithContext ctx) {
        String posixRegex = ".*" + LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()) + "$";
        return builder.call(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                propertyKey,
                builder.literal(posixRegex));
    }

    @Override
    public RexNode visitTraversalPredicate_notEndingWith(
            GremlinGSParser.TraversalPredicate_notEndingWithContext ctx) {
        String posixRegex = ".*" + LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()) + "$";
        return builder.not(
                builder.call(
                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                        propertyKey,
                        builder.literal(posixRegex)));
    }

    @Override
    public RexNode visitTraversalPredicate_containing(
            GremlinGSParser.TraversalPredicate_containingContext ctx) {
        String posixRegex = ".*" + LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()) + ".*";
        return builder.call(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                propertyKey,
                builder.literal(posixRegex));
    }

    @Override
    public RexNode visitTraversalPredicate_notContaining(
            GremlinGSParser.TraversalPredicate_notContainingContext ctx) {
        String posixRegex = ".*" + LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()) + ".*";
        return builder.not(
                builder.call(
                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                        propertyKey,
                        builder.literal(posixRegex)));
    }
}
