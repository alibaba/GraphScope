package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;


public class CustomGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal.Admin<S, E> {

    public GraphTraversal<S, Vertex> process(final String identifier) {
        this.asAdmin().getBytecode().addStep("process", identifier);
        return this.asAdmin().addStep(new StringProcessStep<S>(this.asAdmin(), identifier));
    }

    public <E2> GraphTraversal<S, E2> process(final Traversal<?, E2> processTraversal) {
        this.asAdmin().getBytecode().addStep("process", processTraversal);
        return this.asAdmin().addStep(new TraversalProcessStep<>(this.asAdmin(), processTraversal));
    }

    public GraphTraversal<S, E> scatter(final String scatterName) {
        this.asAdmin().getBytecode().addStep("scatter", scatterName);
        return this.asAdmin().addStep(new ScatterStep<S, E>(this.asAdmin(), scatterName));
    }

    public GraphTraversal<S, Vertex> gather(final String gatherName, final Operator op) {
        this.asAdmin().getBytecode().addStep("gather", gatherName, op);
        return this.asAdmin().addStep(new GatherStep<S>(this.asAdmin(), gatherName, op));
    }

    public GraphTraversal<S, String> expr(final String expression) {
        this.asAdmin().getBytecode().addStep("expr", expression);
        return this.asAdmin().addStep(new ExprStep<S>(this.asAdmin(), expression));
    }

    public CustomGraphTraversal() {
        super();
    }

    public CustomGraphTraversal(final GraphTraversalSource graphTraversalSource) {
        super(graphTraversalSource);
    }

    public CustomGraphTraversal(final Graph graph) {
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
    public CustomGraphTraversal<S, E> clone() {
        return (CustomGraphTraversal<S, E>) super.clone();
    }
}