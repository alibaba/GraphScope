package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public abstract class DummyGraph implements Graph {
    @Override
    public Vertex addVertex(Object... keyValues) {
        return null;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException();
    }
}
