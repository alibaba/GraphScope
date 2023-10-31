/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.plugin.traversal;

import com.google.common.collect.Maps;

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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Map;

public class IrCustomizedTraversalSource extends GraphTraversalSource {
    private @Nullable Map<String, String> queryConfigs;

    public IrCustomizedTraversalSource(
            final Graph graph, final TraversalStrategies traversalStrategies) {
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
        clone.queryConfigs = Maps.newHashMap();
        return clone;
    }

    @Override
    public IrCustomizedTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        bytecode.addStep("V", vertexIds);
        Admin<Vertex, Vertex> traversal = new IrCustomizedTraversal(this);
        return (IrCustomizedTraversal<Vertex, Vertex>)
                traversal.addStep(new GraphStep(traversal, Vertex.class, true, vertexIds));
    }

    @Override
    public IrCustomizedTraversal<Edge, Edge> E(final Object... edgesIds) {
        bytecode.addStep("E", edgesIds);
        Admin<Edge, Edge> traversal = new IrCustomizedTraversal(this);
        return (IrCustomizedTraversal<Edge, Edge>)
                traversal.addStep(new GraphStep(traversal, Edge.class, true, edgesIds));
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

    public @Nullable Map<String, String> getConfigs() {
        if (this.queryConfigs != null) {
            return Collections.unmodifiableMap(this.queryConfigs);
        }
        return null;
    }

    public void addConfig(String key, Object value) {
        if (this.queryConfigs != null) {
            this.queryConfigs.put(key, String.valueOf(value));
        }
    }
}
