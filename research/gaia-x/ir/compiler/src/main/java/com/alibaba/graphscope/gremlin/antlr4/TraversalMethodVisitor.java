package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.exception.UnsupportedAntlrException;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TraversalMethodVisitor extends TraversalRootVisitor<GraphTraversal> {
    final GraphTraversal graphTraversal;

    public TraversalMethodVisitor(final GremlinGS_0_2BaseVisitor<GraphTraversalSource> gvisitor,
                                  final GraphTraversal graphTraversal) {
        super(gvisitor);
        this.graphTraversal = graphTraversal;
    }

    @Override
    public Traversal visitTraversalMethod(GremlinGS_0_2Parser.TraversalMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Traversal visitTraversalMethod_hasLabel(GremlinGS_0_2Parser.TraversalMethod_hasLabelContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else if (ctx.stringLiteralList() != null) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        } else {
            throw new UnsupportedAntlrException(ctx.getClass(),
                    "supported pattern is [hasLabel('str')] or hasLabel('str1', ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_hasId(GremlinGS_0_2Parser.TraversalMethod_hasIdContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.genericLiteralList() != null) {
            return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()),
                    GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
        } else {
            throw new UnsupportedAntlrException(ctx.getClass(),
                    "supported pattern is [hasId(1)] or hasId(1, 2, ...)");
        }
    }

    @Override
    public Traversal visitTraversalMethod_has(GremlinGS_0_2Parser.TraversalMethod_hasContext ctx) {
        if (ctx.genericLiteral() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
        } else if (ctx.traversalPredicate() != null) {
            return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            throw new UnsupportedAntlrException(ctx.getClass(),
                    "supported pattern is [has('key', 'value')] or [has('key', P)]");
        }
    }

    @Override
    public Traversal visitTraversalMethod_out(GremlinGS_0_2Parser.TraversalMethod_outContext ctx) {
        return graphTraversal.out(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_in(GremlinGS_0_2Parser.TraversalMethod_inContext ctx) {
        return graphTraversal.in(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public Traversal visitTraversalMethod_both(GremlinGS_0_2Parser.TraversalMethod_bothContext ctx) {
        return graphTraversal.both(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    public Traversal visitTraversalMethod_outE(GremlinGS_0_2Parser.TraversalMethod_outEContext ctx) {
        GraphTraversal traversal = graphTraversal.outE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_inV() != null) {
            return visitTraversalMethod_inV(ctx.traversalMethod_inV());
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_inE(GremlinGS_0_2Parser.TraversalMethod_inEContext ctx) {
        GraphTraversal traversal = graphTraversal.inE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_outV() != null) {
            return graphTraversal.outV();
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_bothE(GremlinGS_0_2Parser.TraversalMethod_bothEContext ctx) {
        GraphTraversal traversal = graphTraversal.bothE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        if (ctx.traversalMethod_otherV() != null) {
            return graphTraversal.otherV();
        } else {
            return traversal;
        }
    }

    @Override
    public Traversal visitTraversalMethod_limit(GremlinGS_0_2Parser.TraversalMethod_limitContext ctx) {
        return graphTraversal.limit((Integer) GenericLiteralVisitor.getInstance().visitIntegerLiteral(ctx.integerLiteral()));
    }

    @Override
    public Traversal visitTraversalMethod_outV(GremlinGS_0_2Parser.TraversalMethod_outVContext ctx) {
        return graphTraversal.outV();
    }

    @Override
    public Traversal visitTraversalMethod_inV(GremlinGS_0_2Parser.TraversalMethod_inVContext ctx) {
        return graphTraversal.inV();
    }

    @Override
    public Traversal visitTraversalMethod_otherV(GremlinGS_0_2Parser.TraversalMethod_otherVContext ctx) {
        return graphTraversal.otherV();
    }
}
