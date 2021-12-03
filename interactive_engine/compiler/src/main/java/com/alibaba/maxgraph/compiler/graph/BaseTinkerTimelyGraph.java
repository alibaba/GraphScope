/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.graph;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public abstract class BaseTinkerTimelyGraph implements Graph {
    private Configuration graphConfig = new BaseConfiguration();

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new UnsupportedOperationException();
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
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration configuration() {
        return graphConfig;
    }
}
