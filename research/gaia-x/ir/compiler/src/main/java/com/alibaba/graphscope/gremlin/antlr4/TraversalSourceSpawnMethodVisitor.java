package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.UnsupportedAntlrException;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TraversalSourceSpawnMethodVisitor extends GremlinGS_0_2BaseVisitor<GraphTraversal> {
    final GraphTraversalSource g;

    public TraversalSourceSpawnMethodVisitor(final GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod(GremlinGS_0_2Parser.TraversalSourceSpawnMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_V(GremlinGS_0_2Parser.TraversalSourceSpawnMethod_VContext ctx) {
        if (ctx.getChildCount() != 3) {
            throw new UnsupportedAntlrException(ctx.getClass(), "supported pattern is [g.V()]");
        }
        return g.V();
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_E(GremlinGS_0_2Parser.TraversalSourceSpawnMethod_EContext ctx) {
        if (ctx.getChildCount() != 3) {
            throw new UnsupportedAntlrException(ctx.getClass(), "supported pattern is [g.E()]");
        }
        return g.E();
    }
}
