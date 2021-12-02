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

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TraversalMethodVisitor extends TraversalRootVisitor<GraphTraversal> {
    final GraphTraversal graphTraversal;

    public TraversalMethodVisitor(final GremlinGSBaseVisitor<GraphTraversalSource> gvisitor,
                                  final GraphTraversal graphTraversal) {
        super(gvisitor);
        this.graphTraversal = graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod(GremlinGSParser.TraversalMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Traversal visitTraversalMethod_hasLabel(GremlinGSParser.TraversalMethod_hasLabelContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else if (ctx.stringLiteralList() != null) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [hasLabel('str')] or hasLabel('str1', ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_hasId(GremlinGSParser.TraversalMethod_hasIdContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.genericLiteralList() != null) {
            return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()),
                    GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [hasId(1)] or hasId(1, 2, ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_has(GremlinGSParser.TraversalMethod_hasContext ctx) {
        if (ctx.genericLiteral() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.traversalPredicate() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(),
                    "supported pattern is [has('key', 'value')] or [has('key', P)]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_out(GremlinGSParser.TraversalMethod_outContext ctx) {
        return graphTraversal.out(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_in(GremlinGSParser.TraversalMethod_inContext ctx) {
        return graphTraversal.in(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_both(GremlinGSParser.TraversalMethod_bothContext ctx) {
        return graphTraversal.both(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    public Traversal visitTraversalMethod_outE(GremlinGSParser.TraversalMethod_outEContext ctx) {
        GraphTraversal traversal = graphTraversal.outE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_inV() != null) {
            return visitTraversalMethod_inV(ctx.traversalMethod_inV());
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_inE(GremlinGSParser.TraversalMethod_inEContext ctx) {
        GraphTraversal traversal = graphTraversal.inE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_outV() != null) {
            return graphTraversal.outV();
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_bothE(GremlinGSParser.TraversalMethod_bothEContext ctx) {
        GraphTraversal traversal = graphTraversal.bothE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_otherV() != null) {
            return graphTraversal.otherV();
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_limit(GremlinGSParser.TraversalMethod_limitContext ctx) {
        return graphTraversal.limit((Integer) GenericLiteralVisitor.getInstance().visitIntegerLiteral(ctx.integerLiteral()));
    }

    @Override
    public Traversal visitTraversalMethod_outV(GremlinGSParser.TraversalMethod_outVContext ctx) {
        return graphTraversal.outV();
    }

    @Override
    public Traversal visitTraversalMethod_inV(GremlinGSParser.TraversalMethod_inVContext ctx) {
        return graphTraversal.inV();
    }

    @Override
    public Traversal visitTraversalMethod_otherV(GremlinGSParser.TraversalMethod_otherVContext ctx) {
        return graphTraversal.otherV();
    }
}
