package com.alibaba.graphscope.gremlin.antlr4;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * This class implements Gremlin grammar's nested-traversal-list methods that returns a {@link Traversal} {@code []}
 * to the callers.
 */
public class NestedTraversalSourceListVisitor extends GremlinGSBaseVisitor<Traversal[]> {

    protected final GremlinGSBaseVisitor<GraphTraversal> tvisitor;

    public NestedTraversalSourceListVisitor(final GremlinGSBaseVisitor<GraphTraversal> tvisitor) {
        this.tvisitor = tvisitor;
    }

    @Override
    public Traversal[] visitNestedTraversalExpr(final GremlinGSParser.NestedTraversalExprContext ctx) {
        final int childCount = ctx.getChildCount();

        // handle arbitrary number of traversals that are separated by comma
        final Traversal[] results = new Traversal[(childCount + 1) / 2];
        int childIndex = 0;
        while (childIndex < ctx.getChildCount()) {
            results[childIndex / 2] = tvisitor.visitNestedTraversal(
                    (GremlinGSParser.NestedTraversalContext) ctx.getChild(childIndex));
            // skip comma child
            childIndex += 2;
        }

        return results;
    }
}