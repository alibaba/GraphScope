/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/language/grammar/TraversalPredicateVisitor.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;

import java.util.Collection;

public class TraversalPredicateVisitor extends GremlinGSBaseVisitor<P> {
    private static TraversalPredicateVisitor instance;

    public static TraversalPredicateVisitor getInstance() {
        if (instance == null) {
            instance = new TraversalPredicateVisitor();
        }
        return instance;
    }

    private TraversalPredicateVisitor() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate(final GremlinGSParser.TraversalPredicateContext ctx) {
        switch (ctx.getChildCount()) {
            case 1:
                // handle simple predicate
                return visitChildren(ctx);
            case 6:
                final int childIndexOfParameterOperator = 2;
                final int childIndexOfCaller = 0;
                final int childIndexOfArgument = 4;

                if (ctx.getChild(childIndexOfParameterOperator).getText().equals("or")) {
                    // handle or
                    return visit(ctx.getChild(childIndexOfCaller))
                            .or(visit(ctx.getChild(childIndexOfArgument)));
                } else {
                    // handle and
                    return visit(ctx.getChild(childIndexOfCaller))
                            .and(visit(ctx.getChild(childIndexOfArgument)));
                }
            default:
                throw new UnsupportedEvalException(
                        ctx.getClass(),
                        "unexpected number of children in TraversalPredicateContext "
                                + ctx.getChildCount());
        }
    }

    /**
     * get 1 generic literal argument from the antlr parse tree context,
     * where the arguments has the child index of 2
     */
    private Object getSingleGenericLiteralArgument(final ParseTree ctx) {
        final int childIndexOfParameterValue = 2;
        return GenericLiteralVisitor.getInstance()
                .visitGenericLiteral(
                        (GremlinGSParser.GenericLiteralContext)
                                ctx.getChild(childIndexOfParameterValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_eq(final GremlinGSParser.TraversalPredicate_eqContext ctx) {
        return P.eq(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_neq(final GremlinGSParser.TraversalPredicate_neqContext ctx) {
        return P.neq(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_lt(final GremlinGSParser.TraversalPredicate_ltContext ctx) {
        return P.lt(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_lte(final GremlinGSParser.TraversalPredicate_lteContext ctx) {
        return P.lte(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_gt(final GremlinGSParser.TraversalPredicate_gtContext ctx) {
        return P.gt(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_gte(final GremlinGSParser.TraversalPredicate_gteContext ctx) {
        return P.gte(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_not(final GremlinGSParser.TraversalPredicate_notContext ctx) {
        final int childIndexOfParameter = 2;
        return P.not(
                visitTraversalPredicate(
                        (GremlinGSParser.TraversalPredicateContext)
                                ctx.getChild(childIndexOfParameter)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_inside(
            final GremlinGSParser.TraversalPredicate_insideContext ctx) {
        final Object[] arguments = getDoubleGenericLiteralArgument(ctx);
        return P.inside(arguments[0], arguments[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_outside(
            final GremlinGSParser.TraversalPredicate_outsideContext ctx) {
        final Object[] arguments = getDoubleGenericLiteralArgument(ctx);
        return P.outside(arguments[0], arguments[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_within(GremlinGSParser.TraversalPredicate_withinContext ctx) {
        if (ctx.genericLiteralList() != null) {
            Object args = GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList());
            P within;
            if (args instanceof Object[]) {
                within = P.within((Object[]) args);
            } else if (args instanceof Collection) {
                within = P.within((Collection) args);
            } else {
                within = P.within(args);
            }
            return within;
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [within('a')] or [within('a', 'b')]");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_without(
            GremlinGSParser.TraversalPredicate_withoutContext ctx) {
        if (ctx.genericLiteralList() != null) {
            Object args = GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList());
            P without;
            if (args instanceof Object[]) {
                without = P.without((Object[]) args);
            } else if (args instanceof Collection) {
                without = P.without((Collection) args);
            } else {
                without = P.without(args);
            }
            return without;
        } else {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern is [without('a')] or [without('a', 'b')]");
        }
    }

    @Override
    public P visitTraversalPredicate_containing(
            final GremlinGSParser.TraversalPredicate_containingContext ctx) {
        return TextP.containing(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_notContaining(
            final GremlinGSParser.TraversalPredicate_notContainingContext ctx) {
        return TextP.notContaining(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_startingWith(
            final GremlinGSParser.TraversalPredicate_startingWithContext ctx) {
        return TextP.startingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_notStartingWith(
            final GremlinGSParser.TraversalPredicate_notStartingWithContext ctx) {
        return TextP.notStartingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_endingWith(
            final GremlinGSParser.TraversalPredicate_endingWithContext ctx) {
        return TextP.endingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_notEndingWith(
            final GremlinGSParser.TraversalPredicate_notEndingWithContext ctx) {
        return TextP.notEndingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * get 2 generic literal arguments from the antlr parse tree context,
     * where the arguments has the child index of 2 and 4
     */
    private Object[] getDoubleGenericLiteralArgument(ParseTree ctx) {
        final int childIndexOfParameterFirst = 2;
        final int childIndexOfParameterSecond = 4;

        final Object first =
                GenericLiteralVisitor.getInstance()
                        .visitGenericLiteral(
                                (GremlinGSParser.GenericLiteralContext)
                                        ctx.getChild(childIndexOfParameterFirst));
        final Object second =
                GenericLiteralVisitor.getInstance()
                        .visitGenericLiteral(
                                (GremlinGSParser.GenericLiteralContext)
                                        ctx.getChild(childIndexOfParameterSecond));

        return new Object[] {first, second};
    }
}
