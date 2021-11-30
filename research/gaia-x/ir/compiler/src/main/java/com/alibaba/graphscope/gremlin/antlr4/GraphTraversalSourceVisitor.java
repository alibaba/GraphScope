package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.UnsupportedAntlrException;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GraphTraversalSourceVisitor extends GremlinGS_0_2BaseVisitor<GraphTraversalSource> {
    private GraphTraversalSource g;

    public GraphTraversalSourceVisitor(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public GraphTraversalSource visitTraversalSource(GremlinGS_0_2Parser.TraversalSourceContext ctx) {
        if (ctx.getChildCount() != 1) {
            throw new UnsupportedAntlrException(ctx.getClass(), "supported pattern of source is [g]");
        }
        return g;
    }
}
