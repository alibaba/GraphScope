package com.alibaba.maxgraph.v2.common.frontend.remote;

import com.alibaba.maxgraph.proto.v2.LabelVertexEdgeId;
import com.alibaba.maxgraph.proto.v2.ScanStoreRequest;
import com.alibaba.maxgraph.proto.v2.VertexLabelIdsRequest;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphQueryDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphReader;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;
import com.alibaba.maxgraph.v2.common.frontend.cache.MaxGraphCache;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Remote max graph reader class, each query will create one graph reader and writer,
 * each reader and writer will belong to only one query, so will can cache the reader/writer
 * result in the instance, and the cache will be release memory when the query is finished
 */
public class RemoteMaxGraphReader implements MaxGraphReader {
    private static final Logger logger = LoggerFactory.getLogger(RemoteMaxGraphReader.class);
    private GraphPartitionManager partitionManager;
    private RoleClients<QueryStoreRpcClient> queryStoreClients;
    private SchemaFetcher schemaFetcher;
    private SnapshotMaxGraph graph;
    private int storeCount;
    private MaxGraphCache cache;

    public RemoteMaxGraphReader(GraphPartitionManager partitionManager,
                                RoleClients<QueryStoreRpcClient> queryStoreClients,
                                SchemaFetcher schemaFetcher,
                                SnapshotMaxGraph graph,
                                int storeCount,
                                MaxGraphCache cache) {
        this.partitionManager = partitionManager;
        this.queryStoreClients = queryStoreClients;
        this.schemaFetcher = schemaFetcher;
        this.graph = graph;
        this.storeCount = storeCount;
        this.cache = cache;
    }

    @Override
    public Vertex getVertex(ElementId vertexId) throws GraphQueryDataException {
        Vertex cached = this.cache.getVertex(vertexId);
        if (null != cached) {
            return cached;
        }

        Iterator<Vertex> vertexIterator = getVertices(Sets.newHashSet(vertexId));
        if (vertexIterator.hasNext()) {
            Vertex vertex = vertexIterator.next();
            this.cache.addVertex(vertexId, vertex);
            return vertex;
        } else {
            throw new GraphQueryDataException("No vertex with id " + vertexId);
        }
    }

    @Override
    public Iterator<Vertex> getVertices(Set<ElementId> vertexIds) throws GraphQueryDataException {
        List<Vertex> resultVertexList = Lists.newArrayListWithCapacity(vertexIds.size());
        Set<ElementId> queryVertexIds = Sets.newHashSet();
        for (ElementId vertexId : vertexIds) {
            Vertex cached = cache.getVertex(vertexId);
            if (null != cached) {
                resultVertexList.add(cached);
            } else {
                queryVertexIds.add(vertexId);
            }
        }
        if (!queryVertexIds.isEmpty()) {
            Map<Integer, VertexLabelIdsRequest.Builder> storeVertexRequestList = buildPartitionVertexRequest(queryVertexIds);
            CountDownLatch latch = new CountDownLatch(storeVertexRequestList.size());
            GraphSchema schema = schemaFetcher.fetchSchema().getSchema();
            StoreVerticesResponseObserver responseObserver = new StoreVerticesResponseObserver(latch, schema, graph);
            for (Map.Entry<Integer, VertexLabelIdsRequest.Builder> entry : storeVertexRequestList.entrySet()) {
                queryStoreClients.getClient(entry.getKey()).getVertexes(entry.getValue().build(), responseObserver);
            }
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new RuntimeException("query vertexes from store timeout");
                }
            } catch (Exception e) {
                throw new GraphQueryDataException("query vertex from store failed", e);
            }
            List<Vertex> queryVertexList = responseObserver.getVertexList();
            for (Vertex vertex : queryVertexList) {
                this.cache.addVertex((ElementId) vertex.id(), vertex);
            }
            resultVertexList.addAll(queryVertexList);
        }

        return resultVertexList.iterator();
    }

    private Map<Integer, VertexLabelIdsRequest.Builder> buildPartitionVertexRequest(Set<ElementId> vertexIds) {
        Map<Integer, VertexLabelIdsRequest.Builder> storeVertexRequestList = Maps.newHashMap();
        SnapshotSchema snapshotSchema = schemaFetcher.fetchSchema();
        for (ElementId elementId : vertexIds) {
            int storeId = partitionManager.getVertexStoreId(elementId.labelId(), elementId.id());
            VertexLabelIdsRequest.Builder vertexRequestBuilder = storeVertexRequestList.get(storeId);
            if (null == vertexRequestBuilder) {
                vertexRequestBuilder = VertexLabelIdsRequest.newBuilder().setSnapshotId(snapshotSchema.getSnapshotId());
                storeVertexRequestList.put(storeId, vertexRequestBuilder);
            }
            vertexRequestBuilder.addVertexId(LabelVertexEdgeId.newBuilder()
                    .setLabelId(elementId.labelId())
                    .setId(elementId.id())
                    .build());
        }
        return storeVertexRequestList;
    }

    private Map<Integer, VertexLabelIdsRequest.Builder> buildPartitionVertexDirectionRequest(Set<ElementId> vertexIds, String... edgeLabels) {
        Map<Integer, VertexLabelIdsRequest.Builder> storeVertexRequestList = Maps.newHashMap();
        SnapshotSchema snapshotSchema = schemaFetcher.fetchSchema();
        for (ElementId elementId : vertexIds) {
            int storeId = partitionManager.getVertexStoreId(elementId.labelId(), elementId.id());
            VertexLabelIdsRequest.Builder vertexRequestBuilder = storeVertexRequestList.get(storeId);
            if (null == vertexRequestBuilder) {
                vertexRequestBuilder = VertexLabelIdsRequest.newBuilder().setSnapshotId(snapshotSchema.getSnapshotId());
                if (null != edgeLabels && edgeLabels.length > 0) {
                    for (String edgeLabel : edgeLabels) {
                        try {
                            vertexRequestBuilder.addEdgeLabels(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
                        } catch (GraphElementNotFoundException ignored) {
                        }
                    }
                    if (vertexRequestBuilder.getEdgeLabelsCount() <= 0) {
                        return Maps.newHashMap();
                    }
                }
                storeVertexRequestList.put(storeId, vertexRequestBuilder);
            }
            vertexRequestBuilder.addVertexId(LabelVertexEdgeId.newBuilder()
                    .setLabelId(elementId.labelId())
                    .setId(elementId.id())
                    .build());
        }
        return storeVertexRequestList;
    }

    @Override
    public Iterator<Vertex> getVertices(Set<ElementId> vertexIds, Direction direction, String... edgeLabels) throws GraphQueryDataException {
        List<Vertex> resultVertexList = null;
        SnapshotSchema snapshotSchema = schemaFetcher.fetchSchema();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            Map<Integer, VertexLabelIdsRequest.Builder> storeVertexRequestList = buildPartitionVertexDirectionRequest(vertexIds, edgeLabels);
            CountDownLatch latch = new CountDownLatch(storeVertexRequestList.size());
            StoreVertexIdsResponseObserver responseObserver = new StoreVertexIdsResponseObserver(latch, snapshotSchema.getSchema(), graph);
            for (Map.Entry<Integer, VertexLabelIdsRequest.Builder> entry : storeVertexRequestList.entrySet()) {
                queryStoreClients.getClient(entry.getKey()).getOutVertexes(entry.getValue().build(), responseObserver);
            }
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new RuntimeException("query vertexes from store timeout");
                }
            } catch (Exception e) {
                throw new GraphQueryDataException("query vertex from store failed", e);
            }
            resultVertexList = responseObserver.getVertexList();
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            VertexLabelIdsRequest.Builder vertexLabelBuilder = VertexLabelIdsRequest.newBuilder()
                    .setSnapshotId(snapshotSchema.getSnapshotId());
            for (ElementId elementId : vertexIds) {
                vertexLabelBuilder.addVertexId(LabelVertexEdgeId.newBuilder()
                        .setId(elementId.id())
                        .setLabelId(elementId.labelId())
                        .build());
            }
            if (null != edgeLabels) {
                for (String edgeLabel : edgeLabels) {
                    vertexLabelBuilder.addEdgeLabels(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
                }
            }
            CountDownLatch latch = new CountDownLatch(this.storeCount);
            StoreVertexIdsResponseObserver responseObserver = new StoreVertexIdsResponseObserver(latch, snapshotSchema.getSchema(), graph);
            for (int i = 0; i < this.storeCount; i++) {
                this.queryStoreClients.getClient(i).getInVertexes(vertexLabelBuilder.build(), responseObserver);
            }
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new RuntimeException("query vertexes from store timeout");
                }
            } catch (Exception e) {
                throw new GraphQueryDataException("query vertex from store failed", e);
            }
            if (null == resultVertexList) {
                resultVertexList = responseObserver.getVertexList();
            } else {
                resultVertexList.addAll(responseObserver.getVertexList());
            }
        }

        assert resultVertexList != null;
        return resultVertexList.iterator();
    }

    @Override
    public Iterator<Vertex> scanVertices(String... vertexLabels) throws GraphQueryDataException {
        SnapshotSchema snapshotSchema = schemaFetcher.fetchSchema();
        ScanStoreRequest.Builder scanBuilder = ScanStoreRequest.newBuilder()
                .setSnapshotId(snapshotSchema.getSnapshotId());
        if (null != vertexLabels) {
            for (String vertexLabel : vertexLabels) {
                try {
                    scanBuilder.addLabelId(snapshotSchema.getSchema().getSchemaElement(vertexLabel).getLabelId());
                } catch (GraphElementNotFoundException ignored) {
                }
            }
        }
        CountDownLatch latch = new CountDownLatch(this.storeCount);
        StoreVerticesResponseObserver responseObserver = new StoreVerticesResponseObserver(latch, snapshotSchema.getSchema(), this.graph);
        for (int i = 0; i < this.storeCount; i++) {
            this.queryStoreClients.getClient(i).scanVertexes(scanBuilder.build(), responseObserver);
        }
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("query vertexes from store timeout");
            }
        } catch (Exception e) {
            throw new GraphQueryDataException("query vertex from store failed", e);
        }

        return responseObserver.getVertexList().iterator();
    }

    @Override
    public Iterator<Edge> getEdges(Set<ElementId> edgeIdList) throws GraphQueryDataException {
        List<Edge> edgeList = Lists.newArrayList();
        if (edgeIdList.isEmpty()) {
            return edgeList.iterator();
        }
        SnapshotSchema snapshotSchema = this.schemaFetcher.fetchSchema();
        Set<String> edgeLabelList = edgeIdList.stream().map(v -> snapshotSchema.getSchema().getSchemaElement(v.labelId()).getLabel()).collect(Collectors.toSet());
        Iterator<Edge> edgeIterator = this.scanEdges(edgeLabelList.toArray(new String[0]));
        while (edgeIterator.hasNext()) {
            Edge edge = edgeIterator.next();
            if (edgeIdList.contains(edge.id())) {
                edgeList.add(edge);
            }
        }
        return edgeList.iterator();
    }

    @Override
    public Iterator<Edge> getEdges(ElementId vertexId, Direction direction, String... edgeLabels) throws GraphQueryDataException {
        List<Edge> resultEdgeList = null;
        SnapshotSchema snapshotSchema = schemaFetcher.fetchSchema();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            Map<Integer, VertexLabelIdsRequest.Builder> storeVertexRequestList = buildPartitionVertexDirectionRequest(Sets.newHashSet(vertexId), edgeLabels);
            CountDownLatch latch = new CountDownLatch(storeVertexRequestList.size());
            StoreTargetEdgesResponseObserver responseObserver = new StoreTargetEdgesResponseObserver(Direction.OUT,
                    vertexId,
                    latch,
                    snapshotSchema.getSchema(),
                    graph);
            for (Map.Entry<Integer, VertexLabelIdsRequest.Builder> entry : storeVertexRequestList.entrySet()) {
                queryStoreClients.getClient(entry.getKey()).getOutEdges(entry.getValue().build(), responseObserver);
            }
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new RuntimeException("query vertexes from store timeout");
                }
            } catch (Exception e) {
                throw new GraphQueryDataException("query vertex from store failed", e);
            }
            resultEdgeList = responseObserver.getEdgeList();
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            VertexLabelIdsRequest.Builder vertexLabelBuilder = VertexLabelIdsRequest.newBuilder()
                    .addVertexId(LabelVertexEdgeId.newBuilder()
                            .setId(vertexId.id())
                            .setLabelId(vertexId.labelId())
                            .build())
                    .setSnapshotId(snapshotSchema.getSnapshotId());
            if (null != edgeLabels) {
                for (String edgeLabel : edgeLabels) {
                    vertexLabelBuilder.addEdgeLabels(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
                }
            }
            CountDownLatch latch = new CountDownLatch(this.storeCount);
            StoreTargetEdgesResponseObserver responseObserver = new StoreTargetEdgesResponseObserver(Direction.IN,
                    vertexId,
                    latch,
                    snapshotSchema.getSchema(),
                    graph);
            for (int i = 0; i < this.storeCount; i++) {
                this.queryStoreClients.getClient(i).getInEdges(vertexLabelBuilder.build(), responseObserver);
            }
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new RuntimeException("query vertexes from store timeout");
                }
            } catch (Exception e) {
                throw new GraphQueryDataException("query vertex from store failed", e);
            }
            if (null == resultEdgeList) {
                resultEdgeList = responseObserver.getEdgeList();
            } else {
                resultEdgeList.addAll(responseObserver.getEdgeList());
            }
        }

        assert resultEdgeList != null;
        return resultEdgeList.iterator();
    }

    @Override
    public Iterator<Edge> scanEdges(String... edgeLabels) throws GraphQueryDataException {
        SnapshotSchema snapshotSchema = schemaFetcher.fetchSchema();
        ScanStoreRequest.Builder scanBuilder = ScanStoreRequest.newBuilder()
                .setSnapshotId(snapshotSchema.getSnapshotId());
        if (null != edgeLabels) {
            for (String edgeLabel : edgeLabels) {
                try {
                    scanBuilder.addLabelId(snapshotSchema.getSchema().getSchemaElement(edgeLabel).getLabelId());
                } catch (GraphElementNotFoundException ignored) {
                }
            }
        }
        CountDownLatch latch = new CountDownLatch(this.storeCount);
        StoreEdgesResponseObserver responseObserver = new StoreEdgesResponseObserver(latch, snapshotSchema.getSchema(), this.graph);
        for (int i = 0; i < this.storeCount; i++) {
            this.queryStoreClients.getClient(i).scanEdges(scanBuilder.build(), responseObserver);
        }
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("query vertexes from store timeout");
            }
        } catch (Exception e) {
            throw new GraphQueryDataException("query vertex from store failed", e);
        }

        return responseObserver.getEdgeList().iterator();
    }
}
