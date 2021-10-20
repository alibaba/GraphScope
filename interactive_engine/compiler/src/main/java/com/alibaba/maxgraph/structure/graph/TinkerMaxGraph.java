/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.structure.graph;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.maxgraph.common.cluster.MaxGraphConfiguration;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.sdkcommon.graph.CancelDataflow;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.sdkcommon.graph.EstimateRequest;
import com.alibaba.maxgraph.sdkcommon.graph.ShowPlanPathListRequest;
import com.alibaba.maxgraph.sdkcommon.graph.ShowProcessListQuery;
import com.alibaba.maxgraph.sdkcommon.graph.StatisticsRequest;
import com.alibaba.maxgraph.structure.MxEdge;
import com.alibaba.maxgraph.structure.MxVertex;
import com.alibaba.maxgraph.tinkerpop.MaxGraphFeatures;
import com.alibaba.maxgraph.tinkerpop.MaxGraphVariables;
import com.alibaba.maxgraph.tinkerpop.Utils;
import com.alibaba.maxgraph.structure.dfs.GraphDfs;
import com.alibaba.maxgraph.tinkerpop.strategies.MxGraphStepStrategy;
import com.alibaba.maxgraph.tinkerpop.traversal.MaxGraphTraversalSource;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import static com.google.common.base.Preconditions.checkArgument;

public class TinkerMaxGraph implements Graph, WrappedGraph<MaxGraph> {
    private MaxGraph graph;
    private MaxGraphConfiguration configuration;
    private GraphDfs graphDfs;
    private Configuration graphConfig = new BaseConfiguration();

    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                TinkerMaxGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class)
                        .clone()
                        .addStrategies(MxGraphStepStrategy.instance()));
    }

    public TinkerMaxGraph(MaxGraphConfiguration configuration, MaxGraph graph, GraphDfs graphDfs) {
        this.graph = graph;
        this.configuration = configuration;
        this.graphDfs = graphDfs;
    }

    /**
     * Add a vertex <code>
     *     graph.addVertex(label, 'person', 'id', 1, 'name', 'jack', 'age', 10)
     * </code>
     *
     * @param keyValues The properties of vertex
     * @return The added vertex
     */
    @Override
    public Vertex addVertex(Object... keyValues) {
        Pair<String, Map<String, Object>> pair = parseVertexProperties(keyValues);
        return new MxVertex(graph.addVertex(pair.getLeft(), pair.getRight()), this);
    }

    public void addVertexAsync(Object... keyValues) {
        Map<Object, Object> keyValueMap = Utils.convertToRawMap(keyValues);
        String label = (String) keyValueMap.remove(T.label);
        if (null == label) {
            label = Vertex.DEFAULT_LABEL;
        }
        Map<String, Object> properties = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : keyValueMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue());
        }
        graph.addVertex(label, properties);
    }

    public void addEdgeAsync(Vertex src, Vertex dst, String label, Object... keyValues) {
        Map<Object, Object> keyValueMap = Utils.convertToRawMap(keyValues);
        Map<String, Object> properties = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : keyValueMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue());
        }
        graph.addEdge(
                label,
                new com.alibaba.maxgraph.structure.Vertex(
                        (ElementId) src.id(), src.label(), null, graph),
                new com.alibaba.maxgraph.structure.Vertex(
                        (ElementId) dst.id(), dst.label(), null, graph),
                properties);
    }

    private Pair<String, Map<String, Object>> parseVertexProperties(Object... keyValues) {
        Map<String, Object> kvs = Utils.convertToMap(keyValues);
        Object labelObj = kvs.remove(T.label.toString());
        if (labelObj == null) {
            labelObj = Vertex.DEFAULT_LABEL;
        }
        final String label = labelObj.toString();
        GraphSchema schema = graph.getSchema();
        GraphElement graphElement = schema.getElement(label);
        if (!(graphElement instanceof GraphVertex)) {
            throw new IllegalArgumentException("Label " + label + " is not vertex");
        }
        List<GraphProperty> primaryKeyList = ((GraphVertex) graphElement).getPrimaryKeyList();
        if (kvs.isEmpty()) {
            for (GraphProperty property : primaryKeyList) {
                kvs.put(property.getName(), property.getDataType().getRandomValue());
            }
        } else {
            for (GraphProperty property : primaryKeyList) {
                if (!kvs.containsKey(property.getName())) {
                    kvs.put(property.getName(), property.getDataType().getRandomValue());
                }
            }
        }
        return Pair.of(label, kvs);
    }

    private CompositeId parseCompositeId(Object id) {
        if (id instanceof Long) {
            return new CompositeId((long) id, 0);
        } else if (id instanceof ElementId) {
            return new CompositeId(((ElementId) id).id(), ((ElementId) id).typeId());
        } else if (id instanceof Vertex) {
            return (CompositeId) ((Vertex) id).id();
        } else {
            String[] idValueList =
                    StringUtils.split(
                            StringUtils.removeEnd(
                                    StringUtils.removeStart(id.toString(), "v["), "]"),
                            ".");
            checkArgument(idValueList.length == 2, "Invalid vertex id format labelId.vertexId");

            return new CompositeId(
                    Long.parseLong(idValueList[1]), Integer.parseInt(idValueList[0]));
        }
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
            throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... ids) {
        Iterator<com.alibaba.maxgraph.structure.Vertex> vertex;
        if (null == ids || ids.length == 0) {
            vertex = this.getBaseGraph().getVertex();
        } else {
            Set<ElementId> idSet = Sets.newHashSet();
            for (Object id : ids) {
                idSet.add(parseCompositeId(id));
            }
            if (idSet.isEmpty()) {
                List<Vertex> emptyList = Lists.newArrayList();
                return emptyList.iterator();
            }
            vertex = this.getBaseGraph().getVertex(idSet);
        }

        return Iterators.transform(vertex, v -> new MxVertex(v, this));
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Iterator<com.alibaba.maxgraph.structure.Edge> edge;
        if (null == edgeIds || edgeIds.length == 0) {
            edge = this.getBaseGraph().getEdges();
        } else {
            throw new UnsupportedOperationException("TinkerMaxGraph.edges(...)");
        }

        return Iterators.transform(edge, e -> new MxEdge(e, this));
    }

    @Override
    public Transaction tx() {
        return new AbstractThreadLocalTransaction(this) {
            boolean open = false;

            @Override
            protected void doOpen() {
                open = true;
            }

            @Override
            protected void doCommit() throws TransactionException {
                try {
                    graph.refresh();
                } catch (Exception e) {
                    throw new TransactionException("do commit failed", e);
                }
            }

            @Override
            protected void doRollback() throws TransactionException {}

            @Override
            public boolean isOpen() {
                return open;
            }
        };
    }

    @Override
    public void close() throws Exception {
        this.getBaseGraph().close();
    }

    @Override
    public Features features() {
        return new MaxGraphFeatures();
    }

    @Override
    public Variables variables() {
        return new MaxGraphVariables();
    }

    @Override
    public Configuration configuration() {
        return graphConfig;
    }

    @Override
    public MaxGraph getBaseGraph() {
        return this.graph;
    }

    public Object dfs(GraphTraversal traversal, long limit) {
        return graphDfs.dfs(traversal.asAdmin(), 0, limit, limit, true);
    }

    public Object dfs(GraphTraversal traversal, long low, long high, long batchSize) {
        return graphDfs.dfs(traversal.asAdmin(), low, high, batchSize, true);
    }

    public Object dfs(GraphTraversal traversal, long limit, boolean order) {
        return graphDfs.dfs(traversal.asAdmin(), 0, limit, limit, order);
    }

    public Object showProcessList() {
        return new ShowProcessListQuery();
    }

    public Object cancel(String queryId) {
        return new CancelDataflow(queryId);
    }

    public GraphSchema schema() {
        return getBaseGraph().getSchema();
    }

    public EstimateRequest estimate() {
        return new EstimateRequest();
    }

    public StatisticsRequest statistics() {
        return new StatisticsRequest();
    }

    public ShowPlanPathListRequest showPlanPathList(GraphTraversal traversal) {
        return new ShowPlanPathListRequest(traversal);
    }

    /**
     * Generate a reusable {@link GraphTraversalSource} instance. The {@link GraphTraversalSource}
     * provides methods for creating {@link
     * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal} instances.
     *
     * @return A graph traversal source
     */
    public GraphTraversalSource traversal() {
        return traversal(MaxGraphTraversalSource.class);
    }
}
