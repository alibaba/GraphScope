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
package com.alibaba.maxgraph.tinkerpop.traversal;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.CompilerConstant;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.graph.CustomSymbols;
import com.alibaba.maxgraph.tinkerpop.steps.CreateGraphStep;
import com.alibaba.maxgraph.tinkerpop.steps.EstimateCountStep;
import com.alibaba.maxgraph.tinkerpop.steps.MaxGraphStep;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
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

    public MaxGraphTraversalSource(final RemoteConnection remoteConnection) {
        this(EmptyGraph.instance(), TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class).clone());
        this.connection = remoteConnection;
        this.strategies.addStrategies(new RemoteStrategy(connection));
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(GraphTraversal.Symbols.V, vertexIds);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultMaxGraphTraversal<>(clone);
        MaxGraphStep maxGraphStep = new MaxGraphStep<>(this.removeQueryConfig(), traversal, Vertex.class, true, vertexIds);
        return traversal.addStep(maxGraphStep);
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(GraphTraversal.Symbols.E, edgesIds);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultMaxGraphTraversal<>(clone);
        MaxGraphStep maxGraphStep = new MaxGraphStep<>(this.removeQueryConfig(), traversal, Edge.class, true, edgesIds);
        return traversal.addStep(maxGraphStep);
    }

    public GraphTraversal<Vertex, Element> estimateVCount(final String... labels) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(CustomSymbols.graph_source);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultMaxGraphTraversal<>(clone);
        return (DefaultMaxGraphTraversal) traversal.addStep(new EstimateCountStep<>(traversal, true, Sets.newHashSet(labels)));
    }

    public GraphTraversal<Edge, Element> estimateECount(final String... labels) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(CustomSymbols.graph_source);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultMaxGraphTraversal<>(clone);
        return (DefaultMaxGraphTraversal) traversal.addStep(new EstimateCountStep<>(traversal, false, Sets.newHashSet(labels)));
    }

    public GraphTraversal<Element, Element> createGraph(final String graphName) {
        final GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(CustomSymbols.graph_source);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultMaxGraphTraversal<>(clone);
        return (DefaultMaxGraphTraversal) traversal.addStep(new CreateGraphStep<>(traversal, graphName));
    }

    private Map<String, Object> removeQueryConfig() {
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
        Map<String, Object> queryConfig = queryConfigList.remove(threadId);
        return null == queryConfig ? Maps.newHashMap() : queryConfig;
    }

    private Map<String, Object> getQueryConfig() {
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
        return queryConfigList.computeIfAbsent(threadId, k -> Maps.newHashMap());
    }

    public MaxGraphTraversalSource config(String key, Object value) {
        this.getQueryConfig().put(key, value);
        return this;
    }

    public MaxGraphTraversalSource timeout(long milliSec) {
        return config(CompilerConstant.QUERY_TIMEOUT_MILLISEC, milliSec);
    }

    public MaxGraphTraversalSource timeoutSec(long sec) {
        return config(CompilerConstant.QUERY_TIMEOUT_MILLISEC, sec * 1000);
    }

    public MaxGraphTraversalSource scheduleVerySmall() {
        return config(CompilerConstant.QUERY_SCHEDULE_GRANULARITY, QueryFlowOuterClass.InputBatchLevel.VerySmall.name());
    }

    public MaxGraphTraversalSource scheduleSmall() {
        return config(CompilerConstant.QUERY_SCHEDULE_GRANULARITY, QueryFlowOuterClass.InputBatchLevel.Small.name());
    }

    public MaxGraphTraversalSource scheduleMedium() {
        return config(CompilerConstant.QUERY_SCHEDULE_GRANULARITY, QueryFlowOuterClass.InputBatchLevel.Medium.name());
    }

    public MaxGraphTraversalSource scheduleLarge() {
        return config(CompilerConstant.QUERY_SCHEDULE_GRANULARITY, QueryFlowOuterClass.InputBatchLevel.Large.name());
    }

    public MaxGraphTraversalSource scheduleVeryLarge() {
        return config(CompilerConstant.QUERY_SCHEDULE_GRANULARITY, QueryFlowOuterClass.InputBatchLevel.VeryLarge.name());
    }

    public MaxGraphTraversalSource enableDebugLog() {
        return config(CompilerConstant.QUERY_DEBUG_LOG_ENABLE, true);
    }

    public MaxGraphTraversalSource disableSnapshot() {
        return config(CompilerConstant.QUERY_DISABLE_SNAPSHOT, true);
    }

    public MaxGraphTraversalSource executeMode(String mode) {
        return config(CompilerConstant.QUERY_EXECUTE_MODE, mode);
    }

    public MaxGraphTraversalSource planPath(int pathIndex) {
        return config(CompilerConstant.QUERY_COSTMODEL_PLAN_PATH, pathIndex);
    }

    public MaxGraphTraversalSource enablePullGraph() {
        return config(CompilerConstant.QUERY_GRAPH_PULL_ENABLE, true);
    }
}
