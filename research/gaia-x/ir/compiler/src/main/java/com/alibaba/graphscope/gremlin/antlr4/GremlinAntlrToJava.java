package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.UnsupportedAntlrException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GremlinAntlrToJava extends GremlinGS_0_2BaseVisitor<Object> {
    final GraphTraversalSource g;

    final GremlinGS_0_2BaseVisitor<GraphTraversalSource> gvisitor;
    final GremlinGS_0_2BaseVisitor<GraphTraversal> tvisitor;

    private static GremlinAntlrToJava instance;

    public static GremlinAntlrToJava getInstance(GraphTraversalSource g) {
        if (instance == null) {
            instance = new GremlinAntlrToJava(g);
        }
        return instance;
    }

    private GremlinAntlrToJava(GraphTraversalSource g) {
        this.g = g;
        this.gvisitor = new GraphTraversalSourceVisitor(this.g);
        this.tvisitor = new TraversalRootVisitor(this.gvisitor);
    }

    @Override
    public Object visitQuery(GremlinGS_0_2Parser.QueryContext ctx) {
        final int childCount = ctx.getChildCount();
        String notice = "supported pattern of query is [g] or [g.V()...]";
        if (childCount != 1) {
            throw new UnsupportedAntlrException(ctx.getClass(), notice);
        }
        final ParseTree firstChild = ctx.getChild(0);

        if (firstChild instanceof GremlinGS_0_2Parser.TraversalSourceContext) {
            return this.gvisitor.visitTraversalSource((GremlinGS_0_2Parser.TraversalSourceContext) firstChild);
        } else if (firstChild instanceof GremlinGS_0_2Parser.RootTraversalContext) {
            return this.tvisitor.visitRootTraversal((GremlinGS_0_2Parser.RootTraversalContext) firstChild);
        } else {
            throw new UnsupportedAntlrException(firstChild.getClass(), notice);
        }
    }
}
