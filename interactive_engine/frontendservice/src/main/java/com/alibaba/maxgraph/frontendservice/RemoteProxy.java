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
package com.alibaba.maxgraph.frontendservice;

import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.iterator.IteratorList;
import com.alibaba.maxgraph.iterator.function.EdgeResponseFunction;
import com.alibaba.maxgraph.iterator.function.VertexResponseFunction;
import com.alibaba.maxgraph.proto.GremlinServiceGrpc;
import com.alibaba.maxgraph.proto.StoreApi;
import com.alibaba.maxgraph.proto.StoreServiceGrpc;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.proto.GremlinQuery.*;
import com.alibaba.maxgraph.structure.Edge;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.graph.MaxGraph;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RemoteProxy implements Closeable {
    private StoreServiceGrpc.StoreServiceBlockingStub stub;
    private final long timeout;
    private ManagedChannel channel;
    private SchemaFetcher schemaFetcher;
    private MaxGraph graph;

    public RemoteProxy(
            String host, int port, long timeout, SchemaFetcher schemaFetcher, MaxGraph graph) {
        this.timeout = timeout;
        this.channel =
                NettyChannelBuilder.forAddress(host, port)
                        .negotiationType(NegotiationType.PLAINTEXT)
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .idleTimeout(1000, TimeUnit.SECONDS)
                        .build();
        this.stub = StoreServiceGrpc.newBlockingStub(channel);
        this.schemaFetcher = schemaFetcher;
        this.graph = graph;
    }

    public Iterator<Vertex> getVertex(final Set<ElementId> ids) {
        return getVertexBlock(ids);
    }

    private Iterator<Vertex> getVertexBlock(Set<ElementId> ids) {
        StoreApi.GetVertexsRequest.Builder b = StoreApi.GetVertexsRequest.newBuilder();
        b.addAllIds(ids.stream().map(ElementId::id).collect(Collectors.toSet()))
                .setSnapshotId(schemaFetcher.getSchemaSnapshotPair().getRight());
        Iterator<VertexResponse> responses =
                stub.withDeadlineAfter(timeout, TimeUnit.SECONDS).getVertexs(b.build());
        List<Iterator<VertexResponse>> responseList = Lists.newArrayList();
        responseList.add(responses);
        return new IteratorList<>(
                responseList,
                new VertexResponseFunction(
                        schemaFetcher.getSchemaSnapshotPair().getLeft(), this.graph));
    }

    public Iterator<Edge> getOutEdges(Set<Vertex> v, String... label) {
        List<Iterator<StoreApi.GraphEdgeReponse>> iterEdgeList = Lists.newArrayList();
        Pair<GraphSchema, Long> schemaPair = schemaFetcher.getSchemaSnapshotPair();
        GraphSchema schema = schemaPair.getLeft();
        long snapshotId = schemaPair.getRight();
        for (Vertex vertex : v) {
            if (label.length == 0) {
                StoreApi.GetOutEdgesRequest.Builder req = StoreApi.GetOutEdgesRequest.newBuilder();
                req.setSnapshotId(snapshotId)
                        .setSrcId(vertex.id.id())
                        .setSnapshotId(schemaPair.getRight());
                Iterator<StoreApi.GraphEdgeReponse> edgeResponse =
                        stub.withDeadlineAfter(timeout, TimeUnit.SECONDS).getOutEdges(req.build());
                iterEdgeList.add(edgeResponse);
            } else {
                for (String labelVal : label) {
                    try {
                        GraphElement element = schema.getElement(labelVal);
                        int labelId = element.getLabelId();
                        StoreApi.GetOutEdgesRequest.Builder req =
                                StoreApi.GetOutEdgesRequest.newBuilder();
                        req.setSnapshotId(snapshotId)
                                .setSrcId(vertex.id.id())
                                .setTypeId(labelId)
                                .setSnapshotId(schemaPair.getRight());
                        Iterator<StoreApi.GraphEdgeReponse> edgeResponse =
                                stub.withDeadlineAfter(timeout, TimeUnit.SECONDS)
                                        .getOutEdges(req.build());
                        iterEdgeList.add(edgeResponse);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        return new IteratorList<>(iterEdgeList, new EdgeResponseFunction(schema, this.graph));
    }

    public Iterator<Edge> getInEdges(Set<Vertex> v, String... label) {
        List<Iterator<StoreApi.GraphEdgeReponse>> iterEdgeList = Lists.newArrayList();
        Pair<GraphSchema, Long> schemaPair = schemaFetcher.getSchemaSnapshotPair();
        GraphSchema schema = schemaPair.getLeft();
        long snapshotId = schemaPair.getRight();
        for (Vertex vertex : v) {
            if (label.length == 0) {
                StoreApi.GetInEdgesRequest.Builder req = StoreApi.GetInEdgesRequest.newBuilder();
                req.setSnapshotId(snapshotId).setDstId(vertex.id.id());
                Iterator<StoreApi.GraphEdgeReponse> edgeResponse =
                        stub.withDeadlineAfter(timeout, TimeUnit.SECONDS).getInEdges(req.build());
                iterEdgeList.add(edgeResponse);
            } else {
                for (String labelVal : label) {
                    try {
                        GraphElement element = schema.getElement(labelVal);
                        int labelId = element.getLabelId();
                        StoreApi.GetInEdgesRequest.Builder req =
                                StoreApi.GetInEdgesRequest.newBuilder();
                        req.setSnapshotId(snapshotId).setDstId(vertex.id.id()).setTypeId(labelId);
                        Iterator<StoreApi.GraphEdgeReponse> edgeResponse =
                                stub.withDeadlineAfter(timeout, TimeUnit.SECONDS)
                                        .getInEdges(req.build());
                        iterEdgeList.add(edgeResponse);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        return new IteratorList<>(iterEdgeList, new EdgeResponseFunction(schema, this.graph));
    }

    public Iterator<Vertex> scan(Set<String> labelList) {
        Pair<GraphSchema, Long> pair = schemaFetcher.getSchemaSnapshotPair();
        Set<Integer> labelIdList = Sets.newHashSet();
        if (null == labelList || labelList.isEmpty()) {
            labelIdList.add(0);
        } else {
            for (String label : labelList) {
                try {
                    labelIdList.add(pair.getLeft().getElement(label).getLabelId());
                } catch (Exception ignored) {
                }
            }
        }
        if (labelIdList.isEmpty()) {
            return new ArrayList<Vertex>().iterator();
        }
        List<Iterator<VertexResponse>> resList = Lists.newArrayList();
        VertexScanRequest vertexScanRequest =
                VertexScanRequest.newBuilder().setTypeId(-1).setOrder(false).build();
        Iterator<VertexResponse> scanResult =
                GremlinServiceGrpc.newBlockingStub(this.channel)
                        .withDeadlineAfter(timeout, TimeUnit.SECONDS)
                        .scan(vertexScanRequest);
        resList.add(scanResult);
        return new IteratorList<>(resList, new VertexResponseFunction(pair.getLeft(), this.graph));
    }

    public Iterator<Vertex> scan() {
        return scan(null);
    }

    public Iterator<Edge> scanEdge() {
        return scanEdge(null);
    }

    public Iterator<Edge> scanEdge(Set<String> labelList) {
        Pair<GraphSchema, Long> pair = schemaFetcher.getSchemaSnapshotPair();
        Set<Integer> labelIdList = Sets.newHashSet();
        if (null == labelList || labelList.isEmpty()) {
            labelIdList.add(0);
        } else {
            for (String label : labelList) {
                try {
                    labelIdList.add(pair.getLeft().getElement(label).getLabelId());
                } catch (Exception ignored) {
                }
            }
        }
        if (labelIdList.isEmpty()) {
            return new ArrayList<Edge>().iterator();
        }
        List<Iterator<StoreApi.GraphEdgeReponse>> resList = Lists.newArrayList();
        for (int labelId : labelIdList) {
            StoreApi.ScanEdgeRequest.Builder req = StoreApi.ScanEdgeRequest.newBuilder();
            req.setSnapshotId(pair.getRight())
                    .setOffset(0)
                    .setLimit(Integer.MAX_VALUE)
                    .setTypeId(labelId);
            resList.add(stub.withDeadlineAfter(timeout, TimeUnit.SECONDS).scanEdges(req.build()));
        }
        return new IteratorList<>(resList, new EdgeResponseFunction(pair.getLeft(), this.graph));
    }

    @Override
    public void close() throws IOException {
        this.stub = null;
        this.channel.shutdown();
    }
}
