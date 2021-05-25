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
package com.alibaba.maxgraph.v2.frontend.graph;

import com.alibaba.maxgraph.v2.frontend.compiler.step.MaxGraphStep;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerConstant;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MaxGraphTraversalSource extends GraphTraversalSource {
    private ConcurrentMap<Long, Map<String, Object>> queryConfigList = new ConcurrentHashMap<>();

    public MaxGraphTraversalSource(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public MaxGraphTraversalSource(final Graph graph) {
        this(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    public MaxGraphTraversalSource(final RemoteConnection connection) {
        this(EmptyGraph.instance(), TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class).clone());
        this.connection = connection;
        this.strategies.addStrategies(new RemoteStrategy(connection));
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(GraphTraversal.Symbols.V, vertexIds);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        MaxGraphStep maxGraphStep = new MaxGraphStep<>(this.removeQueryConfig(), traversal, Vertex.class, true, vertexIds);
        return traversal.addStep(maxGraphStep);
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(GraphTraversal.Symbols.E, edgesIds);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        MaxGraphStep maxGraphStep = new MaxGraphStep<>(this.removeQueryConfig(), traversal, Edge.class, true, edgesIds);
        return traversal.addStep(maxGraphStep);
    }

    private Map<String, Object> getQueryConfig() {
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
        return queryConfigList.computeIfAbsent(threadId, k -> Maps.newHashMap());
    }

    private Map<String, Object> removeQueryConfig() {
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
        Map<String, Object> queryConfig = queryConfigList.remove(threadId);
        return null == queryConfig ? Maps.newHashMap() : queryConfig;
    }

    public MaxGraphTraversalSource timeout(long milliSec) {
        this.getQueryConfig().put(CompilerConstant.QUERY_TIMEOUT_MILLISEC, milliSec);
        return this;
    }

    public MaxGraphTraversalSource timeoutSec(long sec) {
        return timeout(sec * 1000);
    }

    public MaxGraphTraversalSource disableSnapshot() {
        this.getQueryConfig().put(CompilerConstant.QUERY_DISABLE_SNAPSHOT, true);
        return this;
    }
}
