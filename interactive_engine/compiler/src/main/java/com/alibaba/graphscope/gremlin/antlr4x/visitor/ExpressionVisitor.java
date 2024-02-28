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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4.GenericLiteralVisitor;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;

import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExpressionVisitor extends GremlinGSBaseVisitor<RexNode> {
    protected final RexNode propertyKey;
    protected final GraphBuilder builder;

    public static final int SIMPLE_PREDICATE_CHILD_COUNT = 1, CONNECTIVE_PREDICATE_CHILD_COUNT = 6;

    public ExpressionVisitor(GraphBuilder builder, RexNode propertyKey) {
        this.builder = Objects.requireNonNull(builder);
        this.propertyKey = Objects.requireNonNull(propertyKey);
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
                builder.literal(
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral())));
    }

    @Override
    public RexNode visitTraversalPredicate_neq(GremlinGSParser.TraversalPredicate_neqContext ctx) {
        return builder.call(
                GraphStdOperatorTable.NOT_EQUALS,
                propertyKey,
                builder.literal(
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral())));
    }

    @Override
    public RexNode visitTraversalPredicate_lt(GremlinGSParser.TraversalPredicate_ltContext ctx) {
        return builder.call(
                GraphStdOperatorTable.LESS_THAN,
                propertyKey,
                builder.literal(
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral())));
    }

    @Override
    public RexNode visitTraversalPredicate_lte(GremlinGSParser.TraversalPredicate_lteContext ctx) {
        return builder.call(
                GraphStdOperatorTable.LESS_THAN_OR_EQUAL,
                propertyKey,
                builder.literal(
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral())));
    }

    @Override
    public RexNode visitTraversalPredicate_gt(GremlinGSParser.TraversalPredicate_gtContext ctx) {
        return builder.call(
                GraphStdOperatorTable.GREATER_THAN,
                propertyKey,
                builder.literal(
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral())));
    }

    @Override
    public RexNode visitTraversalPredicate_gte(GremlinGSParser.TraversalPredicate_gteContext ctx) {
        return builder.call(
                GraphStdOperatorTable.GREATER_THAN_OR_EQUAL,
                propertyKey,
                builder.literal(
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral())));
    }

    @Override
    public RexNode visitTraversalPredicate_within(
            GremlinGSParser.TraversalPredicate_withinContext ctx) {
        Object[] points = GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList());
        return builder.getRexBuilder()
                .makeIn(
                        propertyKey,
                        Arrays.asList(points).stream()
                                .map(k -> builder.literal(k))
                                .collect(Collectors.toList()));
    }

    @Override
    public RexNode visitTraversalPredicate_without(
            GremlinGSParser.TraversalPredicate_withoutContext ctx) {
        Object[] points = GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList());
        return builder.not(
                builder.getRexBuilder()
                        .makeIn(
                                propertyKey,
                                Arrays.asList(points).stream()
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
        Number lower =
                (Number)
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral(0));
        Number upper =
                (Number)
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral(1));
        return builder.getRexBuilder()
                .makeBetween(
                        propertyKey,
                        builder.literal(new BigDecimal(lower.longValue() + 1)),
                        builder.literal(new BigDecimal(upper.longValue() - 1)));
    }

    @Override
    public RexNode visitTraversalPredicate_outside(
            GremlinGSParser.TraversalPredicate_outsideContext ctx) {
        Number lower =
                (Number)
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral(0));
        Number upper =
                (Number)
                        GenericLiteralVisitor.getInstance()
                                .visitGenericLiteral(ctx.genericLiteral(1));
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
        String posixRegex = "^" + GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
        return builder.call(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                propertyKey,
                builder.literal(posixRegex));
    }

    @Override
    public RexNode visitTraversalPredicate_notStartingWith(
            GremlinGSParser.TraversalPredicate_notStartingWithContext ctx) {
        String posixRegex = "^" + GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
        return builder.not(
                builder.call(
                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                        propertyKey,
                        builder.literal(posixRegex)));
    }

    @Override
    public RexNode visitTraversalPredicate_endingWith(
            GremlinGSParser.TraversalPredicate_endingWithContext ctx) {
        String posixRegex = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()) + "$";
        return builder.call(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                propertyKey,
                builder.literal(posixRegex));
    }

    @Override
    public RexNode visitTraversalPredicate_notEndingWith(
            GremlinGSParser.TraversalPredicate_notEndingWithContext ctx) {
        String posixRegex = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()) + "$";
        return builder.not(
                builder.call(
                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                        propertyKey,
                        builder.literal(posixRegex)));
    }

    @Override
    public RexNode visitTraversalPredicate_containing(
            GremlinGSParser.TraversalPredicate_containingContext ctx) {
        String posixRegex =
                ".*" + GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()) + ".*";
        return builder.call(
                GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                propertyKey,
                builder.literal(posixRegex));
    }

    @Override
    public RexNode visitTraversalPredicate_notContaining(
            GremlinGSParser.TraversalPredicate_notContainingContext ctx) {
        String posixRegex =
                ".*" + GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()) + ".*";
        return builder.not(
                builder.call(
                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                        propertyKey,
                        builder.literal(posixRegex)));
    }
}
