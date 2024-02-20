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
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4.GenericLiteralVisitor;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class WherePredicateVisitor extends ExpressionVisitor {
    private final Ring whereByRing;

    public WherePredicateVisitor(
            GraphBuilder builder, @Nullable String startTag, Ring wherebyRing) {
        super(builder, visitTraversalMethod_whereby(builder, startTag, wherebyRing.next()));
        this.whereByRing = wherebyRing;
    }

    @Override
    public RexNode visitTraversalPredicate_eq(GremlinGSParser.TraversalPredicate_eqContext ctx) {
        return builder.call(
                GraphStdOperatorTable.EQUALS,
                propertyKey,
                visitTraversalMethod_whereby(
                        builder,
                        (String)
                                GenericLiteralVisitor.getInstance()
                                        .visitGenericLiteral(ctx.genericLiteral()),
                        whereByRing.next()));
    }

    @Override
    public RexNode visitTraversalPredicate_neq(GremlinGSParser.TraversalPredicate_neqContext ctx) {
        return builder.call(
                GraphStdOperatorTable.NOT_EQUALS,
                propertyKey,
                visitTraversalMethod_whereby(
                        builder,
                        (String)
                                GenericLiteralVisitor.getInstance()
                                        .visitGenericLiteral(ctx.genericLiteral()),
                        whereByRing.next()));
    }

    @Override
    public RexNode visitTraversalPredicate_lt(GremlinGSParser.TraversalPredicate_ltContext ctx) {
        return builder.call(
                GraphStdOperatorTable.LESS_THAN,
                propertyKey,
                visitTraversalMethod_whereby(
                        builder,
                        (String)
                                GenericLiteralVisitor.getInstance()
                                        .visitGenericLiteral(ctx.genericLiteral()),
                        whereByRing.next()));
    }

    @Override
    public RexNode visitTraversalPredicate_lte(GremlinGSParser.TraversalPredicate_lteContext ctx) {
        return builder.call(
                GraphStdOperatorTable.LESS_THAN_OR_EQUAL,
                propertyKey,
                visitTraversalMethod_whereby(
                        builder,
                        (String)
                                GenericLiteralVisitor.getInstance()
                                        .visitGenericLiteral(ctx.genericLiteral()),
                        whereByRing.next()));
    }

    @Override
    public RexNode visitTraversalPredicate_gt(GremlinGSParser.TraversalPredicate_gtContext ctx) {
        return builder.call(
                GraphStdOperatorTable.GREATER_THAN,
                propertyKey,
                visitTraversalMethod_whereby(
                        builder,
                        (String)
                                GenericLiteralVisitor.getInstance()
                                        .visitGenericLiteral(ctx.genericLiteral()),
                        whereByRing.next()));
    }

    @Override
    public RexNode visitTraversalPredicate_gte(GremlinGSParser.TraversalPredicate_gteContext ctx) {
        return builder.call(
                GraphStdOperatorTable.GREATER_THAN_OR_EQUAL,
                propertyKey,
                visitTraversalMethod_whereby(
                        builder,
                        (String)
                                GenericLiteralVisitor.getInstance()
                                        .visitGenericLiteral(ctx.genericLiteral()),
                        whereByRing.next()));
    }

    @Override
    public RexNode visitTraversalPredicate_within(
            GremlinGSParser.TraversalPredicate_withinContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_without(
            GremlinGSParser.TraversalPredicate_withoutContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_inside(
            GremlinGSParser.TraversalPredicate_insideContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_outside(
            GremlinGSParser.TraversalPredicate_outsideContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_startingWith(
            GremlinGSParser.TraversalPredicate_startingWithContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_notStartingWith(
            GremlinGSParser.TraversalPredicate_notStartingWithContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_endingWith(
            GremlinGSParser.TraversalPredicate_endingWithContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_notEndingWith(
            GremlinGSParser.TraversalPredicate_notEndingWithContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_containing(
            GremlinGSParser.TraversalPredicate_containingContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    @Override
    public RexNode visitTraversalPredicate_notContaining(
            GremlinGSParser.TraversalPredicate_notContainingContext ctx) {
        throw new UnsupportedEvalException(
                ctx.getClass(), ctx.getText() + " is unsupported in where predicate");
    }

    private static RexNode visitTraversalMethod_whereby(
            GraphBuilder builder,
            @Nullable String tag,
            GremlinGSParser.TraversalMethod_wherebyContext byCtx) {
        if (byCtx == null || byCtx.getChildCount() == 3) { // by()
            return builder.variable(tag);
        } else if (byCtx.stringLiteral() != null) {
            return builder.variable(
                    tag, GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
        } else if (byCtx.traversalMethod_values() != null) {
            return builder.variable(
                    tag,
                    GenericLiteralVisitor.getStringLiteral(
                            byCtx.traversalMethod_values().stringLiteral()));
        } else if (byCtx.nestedTraversal() != null) {
            return Utils.convertExprToPair(
                            new NestedTraversalVisitor(builder, tag)
                                    .visitNestedTraversal(byCtx.nestedTraversal()))
                    .getValue0();
        }
        throw new UnsupportedEvalException(
                byCtx.getClass(), byCtx.getText() + " is unsupported in where predicate");
    }

    public static class Ring {
        private final List<GremlinGSParser.TraversalMethod_wherebyContext> byCtxs;
        private int index;

        public Ring(GremlinGSParser.TraversalMethod_whereby_listContext listCtx) {
            this.byCtxs =
                    listCtx == null
                            ? ImmutableList.of()
                            : listCtx.getRuleContexts(
                                    GremlinGSParser.TraversalMethod_wherebyContext.class);
            this.index = 0;
        }

        public GremlinGSParser.TraversalMethod_wherebyContext next() {
            if (byCtxs.isEmpty()) return null;
            return byCtxs.get((index++) % byCtxs.size());
        }
    }
}
