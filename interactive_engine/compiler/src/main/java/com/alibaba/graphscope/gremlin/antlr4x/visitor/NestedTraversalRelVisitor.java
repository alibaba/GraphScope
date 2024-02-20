package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * convert sub traversal nested in {@code NestedTraversalContext} to RelNode
 */
public class NestedTraversalRelVisitor extends GremlinGSBaseVisitor<RelNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;
    private final List<String> tags;

    public NestedTraversalRelVisitor(GraphBuilder parentBuilder) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
        this.tags = Lists.newArrayList();
    }

    public void addTags(List<String> tags) {
        this.tags.addAll(tags);
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
