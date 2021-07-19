package org.apache.tinkperpop.gremlin.groovy.custom;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Admin;
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

public class CustomGraphTraversalSource extends GraphTraversalSource {
    public Optional<Class> getAnonymousTraversalClass() {
        return Optional.of(___.class);
    }

    public CustomGraphTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public CustomGraphTraversalSource(final Graph graph) {
        this(graph, GlobalCache.getStrategies(graph.getClass()));
    }

    public CustomGraphTraversalSource(final RemoteConnection connection) {
        super(connection);
    }

    public CustomGraphTraversalSource clone() {
            CustomGraphTraversalSource clone = (CustomGraphTraversalSource)super.clone();
            clone.strategies = this.strategies.clone();
            clone.bytecode = this.bytecode.clone();
            return clone;
    }

//    public GraphTraversal<Vertex, Vertex> expr(final String expression) {
//        final CustomGraphTraversalSource clone = this.clone();
//        clone.bytecode.addStep("expr", expression);
//        final GraphTraversal.Admin<Vertex, Vertex> traversal = new CustomGraphTraversal<>(clone);
//        return traversal.addStep(new ExprStep<>(traversal, expression));
//    }

    public CustomGraphTraversalSource with(final String key) {
        return (CustomGraphTraversalSource)super.with(key);
    }

    public CustomGraphTraversalSource with(final String key, final Object value) {
        return (CustomGraphTraversalSource)super.with(key, value);
    }

    public CustomGraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (CustomGraphTraversalSource)super.withStrategies(traversalStrategies);
    }

    public CustomGraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (CustomGraphTraversalSource)super.withoutStrategies(traversalStrategyClasses);
    }

    public CustomGraphTraversalSource withComputer(final Computer computer) {
        return (CustomGraphTraversalSource)super.withComputer(computer);
    }

    public CustomGraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return (CustomGraphTraversalSource)super.withComputer(graphComputerClass);
    }

    public CustomGraphTraversalSource withComputer() {
        return (CustomGraphTraversalSource)super.withComputer();
    }

    public <A> CustomGraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        return (CustomGraphTraversalSource)super.withSideEffect(key, initialValue, reducer);
    }

    public <A> CustomGraphTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        return (CustomGraphTraversalSource)super.withSideEffect(key, initialValue, reducer);
    }

    public <A> CustomGraphTraversalSource withSideEffect(final String key, final A initialValue) {
        return (CustomGraphTraversalSource)super.withSideEffect(key, initialValue);
    }

    public <A> CustomGraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        return (CustomGraphTraversalSource)super.withSideEffect(key, initialValue);
    }

    public <A> CustomGraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (CustomGraphTraversalSource)super.withSack(initialValue, splitOperator, mergeOperator);
    }

    public <A> CustomGraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (CustomGraphTraversalSource)super.withSack(initialValue, splitOperator, mergeOperator);
    }

    public <A> CustomGraphTraversalSource withSack(final A initialValue) {
        return (CustomGraphTraversalSource)super.withSack(initialValue);
    }

    public <A> CustomGraphTraversalSource withSack(final Supplier<A> initialValue) {
        return (CustomGraphTraversalSource)super.withSack(initialValue);
    }

    public <A> CustomGraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return (CustomGraphTraversalSource)super.withSack(initialValue, splitOperator);
    }

    public <A> CustomGraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return (CustomGraphTraversalSource)super.withSack(initialValue, splitOperator);
    }

    public <A> CustomGraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return (CustomGraphTraversalSource)super.withSack(initialValue, mergeOperator);
    }

    public <A> CustomGraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return (CustomGraphTraversalSource)super.withSack(initialValue, mergeOperator);
    }

    public CustomGraphTraversalSource withBulk(final boolean useBulk) {
        if (useBulk) {
            return this;
        } else {
            CustomGraphTraversalSource clone = this.clone();
            RequirementsStrategy.addRequirements(clone.getStrategies(), new TraverserRequirement[]{TraverserRequirement.ONE_BULK});
            clone.bytecode.addSource("withBulk", new Object[]{useBulk});
            return clone;
        }
    }

    public CustomGraphTraversalSource withPath() {
        CustomGraphTraversalSource clone = this.clone();
        RequirementsStrategy.addRequirements(clone.getStrategies(), new TraverserRequirement[]{TraverserRequirement.PATH});
        clone.bytecode.addSource("withPath", new Object[0]);
        return clone;
    }

    /** @deprecated */
    @Deprecated
    public CustomGraphTraversalSource withRemote(final Configuration conf) {
        return (CustomGraphTraversalSource)super.withRemote(conf);
    }

    /** @deprecated */
    @Deprecated
    public CustomGraphTraversalSource withRemote(final String configFile) throws Exception {
        return (CustomGraphTraversalSource)super.withRemote(configFile);
    }

    /** @deprecated */
    @Deprecated
    public CustomGraphTraversalSource withRemote(final RemoteConnection connection) {
        if (this.connection != null) {
            throw new IllegalStateException(String.format("TraversalSource already configured with a RemoteConnection [%s]", connection));
        } else {
            CustomGraphTraversalSource clone = this.clone();
            clone.connection = connection;
            clone.getStrategies().addStrategies(new TraversalStrategy[]{new RemoteStrategy(connection)});
            return clone;
        }
    }

    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("addV", new Object[]{label});
        Admin<Vertex, Vertex> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, label));
    }

    public GraphTraversal<Vertex, Vertex> addV(final Traversal<?, String> vertexLabelTraversal) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("addV", new Object[]{vertexLabelTraversal});
        Admin<Vertex, Vertex> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, vertexLabelTraversal));
    }

    public GraphTraversal<Vertex, Vertex> addV() {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("addV", new Object[0]);
        Admin<Vertex, Vertex> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, (String)null));
    }

    public GraphTraversal<Edge, Edge> addE(final String label) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("addE", new Object[]{label});
        Admin<Edge, Edge> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new AddEdgeStartStep(traversal, label));
    }

    public GraphTraversal<Edge, Edge> addE(final Traversal<?, String> edgeLabelTraversal) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("addE", new Object[]{edgeLabelTraversal});
        Admin<Edge, Edge> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new AddEdgeStartStep(traversal, edgeLabelTraversal));
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("inject", starts);
        Admin<S, S> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new InjectStep(traversal, starts));
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("V", vertexIds);
        Admin<Vertex, Vertex> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new GraphStep(traversal, Vertex.class, true, vertexIds));
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("E", edgesIds);
        Admin<Edge, Edge> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new GraphStep(traversal, Edge.class, true, edgesIds));
    }

    public <S> GraphTraversal<S, S> io(final String file) {
        CustomGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep("io", new Object[]{file});
        Admin<S, S> traversal = new CustomGraphTraversal(clone);
        return traversal.addStep(new IoStep(traversal, file));
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

    public static final class Symbols {
        public static final String withBulk = "withBulk";
        public static final String withPath = "withPath";

        private Symbols() {
        }
    }
}

