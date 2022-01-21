package com.alibaba.graphscope.gremlin.plugin.traversal;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

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

    public IrCustomizedTraversalSource clone() {
        IrCustomizedTraversalSource clone = (IrCustomizedTraversalSource) super.clone();
        clone.strategies = this.strategies.clone();
        clone.bytecode = this.bytecode.clone();
        return clone;
    }

    public IrCustomizedTraversalSource with(final String key) {
        return (IrCustomizedTraversalSource) super.with(key);
    }

    public IrCustomizedTraversalSource with(final String key, final Object value) {
        return (IrCustomizedTraversalSource) super.with(key, value);
    }

    public IrCustomizedTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (IrCustomizedTraversalSource) super.withStrategies(traversalStrategies);
    }

    public IrCustomizedTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (IrCustomizedTraversalSource) super.withoutStrategies(traversalStrategyClasses);
    }

    public IrCustomizedTraversalSource withComputer(final Computer computer) {
        return (IrCustomizedTraversalSource) super.withComputer(computer);
    }

    public IrCustomizedTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return (IrCustomizedTraversalSource) super.withComputer(graphComputerClass);
    }

    public IrCustomizedTraversalSource withComputer() {
        return (IrCustomizedTraversalSource) super.withComputer();
    }

    public <A> IrCustomizedTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        return (IrCustomizedTraversalSource) super.withSideEffect(key, initialValue, reducer);
    }

    public <A> IrCustomizedTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        return (IrCustomizedTraversalSource) super.withSideEffect(key, initialValue, reducer);
    }

    public <A> IrCustomizedTraversalSource withSideEffect(final String key, final A initialValue) {
        return (IrCustomizedTraversalSource) super.withSideEffect(key, initialValue);
    }

    public <A> IrCustomizedTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        return (IrCustomizedTraversalSource) super.withSideEffect(key, initialValue);
    }

    public <A> IrCustomizedTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue, splitOperator, mergeOperator);
    }

    public <A> IrCustomizedTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue, splitOperator, mergeOperator);
    }

    public <A> IrCustomizedTraversalSource withSack(final A initialValue) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue);
    }

    public <A> IrCustomizedTraversalSource withSack(final Supplier<A> initialValue) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue);
    }

    public <A> IrCustomizedTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue, splitOperator);
    }

    public <A> IrCustomizedTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue, splitOperator);
    }

    public <A> IrCustomizedTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue, mergeOperator);
    }

    public <A> IrCustomizedTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return (IrCustomizedTraversalSource) super.withSack(initialValue, mergeOperator);
    }

    public IrCustomizedTraversalSource withBulk(final boolean useBulk) {
        if (useBulk) {
            return this;
        } else {
            IrCustomizedTraversalSource clone = this.clone();
            RequirementsStrategy.addRequirements(clone.getStrategies(), new TraverserRequirement[]{TraverserRequirement.ONE_BULK});
            clone.bytecode.addSource("withBulk", new Object[]{useBulk});
            return clone;
        }
    }

    public IrCustomizedTraversalSource withPath() {
        IrCustomizedTraversalSource clone = this.clone();
        RequirementsStrategy.addRequirements(clone.getStrategies(), new TraverserRequirement[]{TraverserRequirement.PATH});
        clone.bytecode.addSource("withPath", new Object[0]);
        return clone;
    }

    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("addV", new Object[]{label});
        Admin<Vertex, Vertex> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<Vertex, Vertex>) traversal.addStep(new AddVertexStartStep(traversal, label));
    }

    public GraphTraversal<Vertex, Vertex> addV(final Traversal<?, String> vertexLabelTraversal) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("addV", new Object[]{vertexLabelTraversal});
        Admin<Vertex, Vertex> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<Vertex, Vertex>) traversal.addStep(new AddVertexStartStep(traversal, vertexLabelTraversal));
    }

    public GraphTraversal<Vertex, Vertex> addV() {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("addV", new Object[0]);
        Admin<Vertex, Vertex> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<Vertex, Vertex>) traversal.addStep(new AddVertexStartStep(traversal, (String) null));
    }

    public GraphTraversal<Edge, Edge> addE(final String label) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("addE", new Object[]{label});
        Admin<Edge, Edge> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<Edge, Edge>) traversal.addStep(new AddEdgeStartStep(traversal, label));
    }

    public GraphTraversal<Edge, Edge> addE(final Traversal<?, String> edgeLabelTraversal) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("addE", new Object[]{edgeLabelTraversal});
        Admin<Edge, Edge> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<Edge, Edge>) traversal.addStep(new AddEdgeStartStep(traversal, edgeLabelTraversal));
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("inject", starts);
        Admin<S, S> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<S, S>) traversal.addStep(new InjectStep(traversal, starts));
    }

    public IrCustomizedTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("V", vertexIds);
        Admin<Vertex, Vertex> traversal = new IrCustomizedTraversal(clone);
        return (IrCustomizedTraversal<Vertex, Vertex>) traversal.addStep(new GraphStep(traversal, Vertex.class, true, vertexIds));
    }

    public IrCustomizedTraversal<Edge, Edge> E(final Object... edgesIds) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("E", edgesIds);
        Admin<Edge, Edge> traversal = new IrCustomizedTraversal(clone);
        return (IrCustomizedTraversal<Edge, Edge>) traversal.addStep(new GraphStep(traversal, Edge.class, true, edgesIds));
    }

    public <S> GraphTraversal<S, S> io(final String file) {
        IrCustomizedTraversalSource clone = this.clone();
        clone.bytecode.addStep("io", new Object[]{file});
        Admin<S, S> traversal = new IrCustomizedTraversal(clone);
        return (GraphTraversal<S, S>) traversal.addStep(new IoStep(traversal, file));
    }

    public Transaction tx() {
        return this.graph.tx();
    }

    public void close() throws Exception {
        if (this.connection != null) {
            this.connection.close();
        }

    }

    public String toString() {
        return StringFactory.traversalSourceString(this);
    }
}
