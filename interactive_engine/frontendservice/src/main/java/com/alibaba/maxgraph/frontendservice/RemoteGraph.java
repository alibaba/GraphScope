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
package com.alibaba.maxgraph.frontendservice;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.iterator.IteratorList;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.InstanceStatus;
import com.alibaba.maxgraph.common.ServerAssignment;
import com.alibaba.maxgraph.common.client.WorkerInfo;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.structure.DefaultGraphPartitioner;
import com.alibaba.maxgraph.structure.Edge;
import com.alibaba.maxgraph.structure.GraphPartitioner;
import com.alibaba.maxgraph.structure.IdManager;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.graph.MaxGraph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.*;

public class RemoteGraph implements MaxGraph {

    private static Logger LOG = LoggerFactory.getLogger(RemoteGraph.class);

    private List<RemoteProxy> proxys = Lists.newArrayList();
    private Map<Integer, Integer> partitionDistri = Maps.newHashMap();
    private GraphPartitioner partitioner;
    private IdManager idManager;
    private SchemaFetcher schemaFetcher;
    private final ClientManager clientManager;
    private InstanceConfig instanceConfig;

    public RemoteGraph(Frontend frontend, SchemaFetcher schemaFetcher) {
        this.clientManager = frontend.getClientManager();
        this.schemaFetcher = schemaFetcher;
        this.instanceConfig = frontend.getInstanceConfig();
    }

    public void addExecutorProxy(Endpoint endpoint) {
        LOG.info("add executor proxy :{}", endpoint);
        proxys.add(new RemoteProxy(endpoint.getIp(), endpoint.getPort(), 120L, this.schemaFetcher, this));
    }

    @VisibleForTesting
    public RemoteGraph(final RemoteProxy proxys, SchemaFetcher schemaFetcher, IdManager idManager) {
        this.clientManager = null;
        this.proxys = Lists.newArrayList(proxys);
        this.schemaFetcher = schemaFetcher;
        this.partitioner = new DefaultGraphPartitioner(schemaFetcher.getPartitionNum());
        this.partitionDistri.put(0, 1);
        this.idManager = idManager;
    }

    @Override
    public void refresh() throws Exception {
        InstanceStatus serverStatuses = clientManager.getServerDataApiClient().getServerStatuses();
        List<RemoteProxy> proxys = new ArrayList<>(serverStatuses.server2WorkerInfo.size());

        Set<WorkerInfo> workerInfoSet = new TreeSet<>(serverStatuses.getWorkInfo(RoleType.EXECUTOR));
        for (WorkerInfo v : workerInfoSet) {
            if (serverStatuses.assignments.containsKey(v.id)) {
                ServerAssignment serverAssignment = serverStatuses.readServerAssignment(v.id);
                serverAssignment.getPartitions().forEach(p -> partitionDistri.put(p, v.id));
                proxys.add(new RemoteProxy(v.endpoint.getIp(), v.endpoint.getPort(), 120L, schemaFetcher, this));
            }
        }

        List<Endpoint> endpointList = workerInfoSet.stream()
                .map(workerInfo -> workerInfo.endpoint)
                .collect(toList());
        LOG.info("proxys: {}", JSON.toJson(endpointList));

        List<RemoteProxy> prevProxyList = this.proxys;
        this.proxys = proxys;
        try {
            for (RemoteProxy remoteProxy : prevProxyList) {
                remoteProxy.close();
            }
        } finally {
            LOG.info("Close all previous remote proxy");
        }
        // TODO : dynamic configuration of different Graph Partitioner;
        this.partitioner = new DefaultGraphPartitioner(schemaFetcher.getPartitionNum());

    }

    @Override
    public Map<Integer, Set<ElementId>> partition(final Set<ElementId> ids) {
        return ids.stream().map(id -> {
            int p = this.partitioner.getPartition(id.id());
            Integer s = this.partitionDistri.get(p);
            return Pair.of(s, id);
        }).collect(groupingBy(Pair::getLeft, mapping(Pair::getRight, toSet())));
    }

    @Override
    public Iterator<Vertex> getVertex(Set<ElementId> id) {
        Map<Integer, Set<ElementId>> classified = partition(id);
        return classified.entrySet().parallelStream()
                .flatMap(e -> {
                    Iterator<Vertex> vertex = proxys.get(e.getKey() - 1).getVertex(e.getValue());
                    return IteratorUtils.stream(vertex);
                })
                .map(v -> {
                    v.setGraph(this);
                    return v;
                })
                .iterator();
    }

    @Override
    public Iterator<Vertex> getVertex(String... label) {
        List<Iterator<Vertex>> vertexList;
        if (null == label || label.length == 0) {
            vertexList = proxys.stream()
                    .map(RemoteProxy::scan)
                    .collect(toList());
        } else {
            Set<String> labelList = Sets.newHashSet(label);
            vertexList = proxys.stream()
                    .map(v -> v.scan(labelList))
                    .collect(toList());
        }

        return new IteratorList<>(vertexList);
    }

    @Override
    public Vertex addVertex(String label, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Vertex> addVertices(List<Pair<String, Map<String, Object>>> vertexList) {
        throw new UnsupportedOperationException();
    }

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
        switch (direction) {
            case OUT: {
                for (RemoteProxy proxy : proxys) {
                    edgeItorList.add(proxy.getOutEdges(v, label));
                }
                break;
            }
            case IN: {
                for (RemoteProxy proxy : proxys) {
                    edgeItorList.add(proxy.getInEdges(v, label));
                }
                break;
            }
            case BOTH: {
                for (RemoteProxy proxy : proxys) {
                    edgeItorList.add(proxy.getOutEdges(v, label));
                }
                for (RemoteProxy proxy : proxys) {
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
        if (label.length > 0) {
            edgeIteratorList = this.proxys.stream().map(RemoteProxy::scanEdge).collect(toList());
        } else {
            final Set<String> edgeLabelList = Sets.newHashSet(label);
            edgeIteratorList = this.proxys.stream().map(v -> v.scanEdge(edgeLabelList)).collect(toList());
        }
        return new IteratorList<>(edgeIteratorList);
    }

    @Override
    public Edge addEdge(String label, Vertex src, Vertex dst, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Edge> addEdges(List<Triple<String, Pair<Vertex, Vertex>, Map<String, Object>>> edgeList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEdge(String label, long edgeId, ElementId srcId, ElementId dstId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateEdge(Vertex src, Vertex dst, String label, long edgeId, Map<String, Object> propertyList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphSchema getSchema() {
        return schemaFetcher.getSchemaSnapshotPair().getLeft();
    }

    @Override
    public void close() throws IOException {
        for (RemoteProxy proxy : proxys) {
            proxy.close();
        }
        this.proxys.clear();
    }
}
