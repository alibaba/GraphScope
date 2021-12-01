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
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.P;

public class TraversalPredicateVisitor extends GremlinGS_0_2BaseVisitor<P> {
    private static TraversalPredicateVisitor instance;

    public static TraversalPredicateVisitor getInstance() {
        if (instance == null) {
            instance = new TraversalPredicateVisitor();
        }
        return instance;
    }

    private TraversalPredicateVisitor() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate(final GremlinGS_0_2Parser.TraversalPredicateContext ctx) {
        if (ctx.getChildCount() != 1) {
            throw new UnsupportedEvalException(ctx.getClass(), "support pattern is [P.predicate(...)]");
        }
        return visitChildren(ctx);
    }

    /**
     * get 1 generic literal argument from the antlr parse tree context,
     * where the arguments has the child index of 2
     */
    private Object getSingleGenericLiteralArgument(final ParseTree ctx) {
        final int childIndexOfParameterValue = 2;

        return GenericLiteralVisitor.getInstance().visitGenericLiteral(
                (GremlinGS_0_2Parser.GenericLiteralContext) ctx.getChild(childIndexOfParameterValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_eq(final GremlinGS_0_2Parser.TraversalPredicate_eqContext ctx) {
        return P.eq(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_neq(final GremlinGS_0_2Parser.TraversalPredicate_neqContext ctx) {
        return P.neq(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_lt(final GremlinGS_0_2Parser.TraversalPredicate_ltContext ctx) {
        return P.lt(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_lte(final GremlinGS_0_2Parser.TraversalPredicate_lteContext ctx) {
        return P.lte(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_gt(final GremlinGS_0_2Parser.TraversalPredicate_gtContext ctx) {
        return P.gt(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate_gte(final GremlinGS_0_2Parser.TraversalPredicate_gteContext ctx) {
        return P.gte(getSingleGenericLiteralArgument(ctx));
    }
}
