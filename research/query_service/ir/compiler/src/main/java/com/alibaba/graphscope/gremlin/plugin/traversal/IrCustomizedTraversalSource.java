package com.alibaba.graphscope.gremlin.plugin.traversal;

import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class IrCustomizedTraversalSource extends GraphTraversalSource {
    public IrCustomizedTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public IrCustomizedTraversalSource(final Graph graph) {
        this(graph, GlobalCache.getStrategies(graph.getClass()));
    }

    public IrCustomizedTraversalSource(final RemoteConnection connection) {
        super(connection);
    }

    @Override
    public IrCustomizedTraversalSource clone() {
        IrCustomizedTraversalSource clone = (IrCustomizedTraversalSource) super.clone();
        clone.strategies = this.strategies.clone();
        clone.bytecode = this.bytecode.clone();
        return clone;
    }

    @Override
    public IrCustomizedTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("V", vertexIds);
        Admin<Vertex, Vertex> traversal = new IrCustomizedTraversal(clone);
        return (IrCustomizedTraversal<Vertex, Vertex>) traversal.addStep(new GraphStep(traversal, Vertex.class, true, vertexIds));
    }

    @Override
    public IrCustomizedTraversal<Edge, Edge> E(final Object... edgesIds) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("E", edgesIds);
        Admin<Edge, Edge> traversal = new IrCustomizedTraversal(clone);
        return (IrCustomizedTraversal<Edge, Edge>) traversal.addStep(new GraphStep(traversal, Edge.class, true, edgesIds));
    }

    @Override
    public void close() throws Exception {
        if (this.connection != null) {
            this.connection.close();
        }

    }

    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }
}
