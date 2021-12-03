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
package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.frontendservice.RemoteProxy;
import com.alibaba.maxgraph.iterator.IteratorList;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.structure.Edge;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.graph.MaxGraph;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.operation.EdgeId;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.operation.VertexId;
import com.alibaba.graphscope.groot.schema.EdgeKind;
import com.alibaba.maxgraph.common.util.PartitionUtils;
import com.alibaba.graphscope.groot.frontend.WriteSessionGenerator;
import com.alibaba.graphscope.groot.frontend.write.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.*;

public class MaxGraphImpl implements MaxGraph, NodeDiscovery.Listener {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphImpl.class);

    private SchemaFetcher schemaFetcher;
    private GraphWriter graphWriter;
    private MetaService metaService;
    private String writeSession;

    private Map<Integer, RemoteProxy> proxys = new ConcurrentHashMap<>();
    private long startEdgeInnerId;

    public MaxGraphImpl(
            NodeDiscovery discovery,
            SchemaFetcher schemaFetcher,
            GraphWriter graphWriter,
            WriteSessionGenerator writeSessionGenerator,
            MetaService metaService) {
        this.schemaFetcher = schemaFetcher;
        this.graphWriter = graphWriter;
        this.writeSession = writeSessionGenerator.newWriteSession();
        this.metaService = metaService;
        discovery.addListener(this);
        startEdgeInnerId = System.nanoTime();
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.EXECUTOR_GRAPH) {
            nodes.forEach(
                    (id, node) -> {
                        this.proxys.put(
                                id,
                                new RemoteProxy(
                                        node.getHost(),
                                        node.getPort(),
                                        120L,
                                        this.schemaFetcher,
                                        this));
                    });
        }
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.EXECUTOR_GRAPH) {
            nodes.keySet()
                    .forEach(
                            k -> {
                                RemoteProxy remove = this.proxys.remove(k);
                                if (remove != null) {
                                    try {
                                        remove.close();
                                    } catch (IOException e) {
                                        logger.warn("close node [" + k + "] failed, ignore", e);
                                    }
                                }
                            });
        }
    }

    @Override
    public void refresh() throws Exception {
        graphWriter.flushLastSnapshot(30000);
    }

    private int getVertexStoreId(long vertexId) {
        int partitionCount = this.metaService.getPartitionCount();
        int partitionId = PartitionUtils.getPartitionIdFromKey(vertexId, partitionCount);
        return this.metaService.getStoreIdByPartition(partitionId);
    }

    @Override
    public Iterator<Vertex> getVertex(Set<ElementId> id) {
        Map<Integer, Set<ElementId>> classified =
                id.stream()
                        .map(
                                v -> {
                                    int storeId = getVertexStoreId(v.id());
                                    return Pair.of(storeId, v);
                                })
                        .collect(groupingBy(Pair::getLeft, mapping(Pair::getRight, toSet())));
        return classified.entrySet().parallelStream()
                .flatMap(
                        e -> {
                            Iterator<Vertex> vertex =
                                    proxys.get(e.getKey()).getVertex(e.getValue());
                            return IteratorUtils.stream(vertex);
                        })
                .map(
                        v -> {
                            v.setGraph(this);
                            return v;
                        })
                .iterator();
    }

    @Override
    public Iterator<Vertex> getVertex(String... label) {
        List<Iterator<Vertex>> vertexList;
        Collection<RemoteProxy> remoteProxies = proxys.values();
        if (null == label || label.length == 0) {
            vertexList = remoteProxies.stream().map(RemoteProxy::scan).collect(toList());
        } else {
            Set<String> labelList = Sets.newHashSet(label);
            vertexList = remoteProxies.stream().map(v -> v.scan(labelList)).collect(toList());
        }

        return new IteratorList<>(vertexList);
    }

    @Override
    public Vertex addVertex(String label, Map<String, Object> properties) {
        VertexRecordKey vertexRecordKey = new VertexRecordKey(label);
        DataRecord dataRecord = new DataRecord(vertexRecordKey, properties);
        WriteRequest writeRequest = new WriteRequest(OperationType.OVERWRITE_VERTEX, dataRecord);
        graphWriter.writeBatch(
                getClass().getCanonicalName(), this.writeSession, Arrays.asList(writeRequest));
        return null;
    }

    @Override
    public List<Vertex> addVertices(List<Pair<String, Map<String, Object>>> vertexList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteVertex(CompositeId vertexId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Edge> getEdges(Vertex v, Direction direction, String... label) {
        HashSet<Vertex> vertices = Sets.newHashSet(v);
        return getEdges(vertices, direction, label);
    }

    @Override
    public Iterator<Edge> getEdges(Set<Vertex> v, Direction direction, String... label) {
        List<Iterator<Edge>> edgeItorList = Lists.newArrayList();
        Collection<RemoteProxy> remoteProxies = proxys.values();
        switch (direction) {
            case OUT:
                {
                    for (RemoteProxy proxy : remoteProxies) {
                        edgeItorList.add(proxy.getOutEdges(v, label));
                    }
                    break;
                }
            case IN:
                {
                    for (RemoteProxy proxy : remoteProxies) {
                        edgeItorList.add(proxy.getInEdges(v, label));
                    }
                    break;
                }
            case BOTH:
                {
                    for (RemoteProxy proxy : remoteProxies) {
                        edgeItorList.add(proxy.getOutEdges(v, label));
                    }
                    for (RemoteProxy proxy : remoteProxies) {
                        edgeItorList.add(proxy.getInEdges(v, label));
                    }
                    break;
                }
        }

        return new IteratorList<>(edgeItorList);
    }

    @Override
    public Iterator<Edge> getEdges(String... label) {
        List<Iterator<Edge>> edgeIteratorList;
        Collection<RemoteProxy> remoteProxies = this.proxys.values();
        if (label.length > 0) {
            edgeIteratorList = remoteProxies.stream().map(RemoteProxy::scanEdge).collect(toList());
        } else {
            final Set<String> edgeLabelList = Sets.newHashSet(label);
            edgeIteratorList =
                    remoteProxies.stream().map(v -> v.scanEdge(edgeLabelList)).collect(toList());
        }
        return new IteratorList<>(edgeIteratorList);
    }

    @Override
    public Edge addEdge(String label, Vertex src, Vertex dst, Map<String, Object> properties) {
        GraphSchema schema = getSchema();
        int edgeLabelId = schema.getElement(label).getLabelId();
        EdgeKind edgeKind =
                EdgeKind.newBuilder()
                        .setEdgeLabelId(new LabelId(edgeLabelId))
                        .setSrcVertexLabelId(new LabelId(src.id.typeId()))
                        .setDstVertexLabelId(new LabelId(dst.id.typeId()))
                        .build();
        long innerId = ++startEdgeInnerId;
        EdgeId edgeId = new EdgeId(new VertexId(src.id.id()), new VertexId(dst.id.id()), innerId);
        EdgeTarget edgeTarget = new EdgeTarget(edgeKind, edgeId);
        DataRecord dataRecord = new DataRecord(edgeTarget, properties);
        WriteRequest writeRequest = new WriteRequest(OperationType.OVERWRITE_EDGE, dataRecord);
        graphWriter.writeBatch(
                getClass().getCanonicalName(), this.writeSession, Arrays.asList(writeRequest));
        return null;
    }

    @Override
    public List<Edge> addEdges(
            List<Triple<String, Pair<Vertex, Vertex>, Map<String, Object>>> edgeList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEdge(String label, long edgeId, ElementId srcId, ElementId dstId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateEdge(
            Vertex src, Vertex dst, String label, long edgeId, Map<String, Object> propertyList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphSchema getSchema() {
        return schemaFetcher.getSchemaSnapshotPair().getLeft();
    }

    @Override
    public Map<Integer, Set<ElementId>> partition(Set<ElementId> elementIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {}
}
