package com.alibaba.graphscope.gremlin.plugin.traversal;

import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class IrCustomizedTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal.Admin<S, E> {
    public IrCustomizedTraversal() {
        super();
    }

    public IrCustomizedTraversal(final GraphTraversalSource graphTraversalSource) {
        super(graphTraversalSource);
    }

    public IrCustomizedTraversal(final Graph graph) {
        super(graph);
    }

    @Override
    public GraphTraversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public GraphTraversal<S, E> iterate() {
        return GraphTraversal.Admin.super.iterate();
    }

    @Override
    public IrCustomizedTraversal<S, E> clone() {
        return (IrCustomizedTraversal<S, E>) super.clone();
    }

    public GraphTraversal<S, Vertex> out(Traversal rangeTraversal, String... labels) {
        this.asAdmin().getBytecode().addStep("flatMap", rangeTraversal, labels);
        return this.asAdmin().addStep(new PathExpandStep(this.asAdmin(), Direction.OUT, rangeTraversal, labels));
    }

    public GraphTraversal<S, Vertex> in(Traversal rangeTraversal, String... labels) {
        this.asAdmin().getBytecode().addStep("flatMap", rangeTraversal, labels);
        return this.asAdmin().addStep(new PathExpandStep(this.asAdmin(), Direction.IN, rangeTraversal, labels));
    }

    public GraphTraversal<S, Vertex> both(Traversal rangeTraversal, String... labels) {
        this.asAdmin().getBytecode().addStep("flatMap", rangeTraversal, labels);
        return this.asAdmin().addStep(new PathExpandStep(this.asAdmin(), Direction.BOTH, rangeTraversal, labels));
    }
}
