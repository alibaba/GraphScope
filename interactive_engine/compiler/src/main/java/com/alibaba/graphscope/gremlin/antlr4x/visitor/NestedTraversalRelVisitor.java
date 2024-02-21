package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;

/**
 * convert sub traversal nested in {@code NestedTraversalContext} to RelNode
 */
public class NestedTraversalRelVisitor extends GremlinGSBaseVisitor<RelNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;

    public NestedTraversalRelVisitor(GraphBuilder parentBuilder) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
    }

    @Override
    public RelNode visitNestedTraversal(GremlinGSParser.NestedTraversalContext ctx) {
        RelNode commonRel =
                requireNonNull(parentBuilder.peek(), "parent builder should not be empty");
        Preconditions.checkArgument(
                commonRel instanceof GraphLogicalSource,
                "match should start from global source vertices");
        nestedBuilder.push(commonRel);
        return new GraphBuilderVisitor(nestedBuilder).visitNestedTraversal(ctx).build();
    }
}
