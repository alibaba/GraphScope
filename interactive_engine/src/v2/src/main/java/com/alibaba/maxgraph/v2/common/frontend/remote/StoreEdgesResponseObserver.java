package com.alibaba.maxgraph.v2.common.frontend.remote;

import com.alibaba.maxgraph.proto.v2.CompositeEdgeId;
import com.alibaba.maxgraph.proto.v2.StoreEdgeResult;
import com.alibaba.maxgraph.proto.v2.StoreEdgesResponse;
import com.alibaba.maxgraph.proto.v2.StorePropertyListPb;
import com.alibaba.maxgraph.proto.v2.StorePropertyPb;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphEdge;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphVertex;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class StoreEdgesResponseObserver implements StreamObserver<StoreEdgesResponse> {
    private static final Logger logger = LoggerFactory.getLogger(StoreEdgesResponseObserver.class);

    private CountDownLatch latch;
    private SnapshotMaxGraph graph;
    private GraphSchema schema;
    private List<Edge> edgeList = Lists.newCopyOnWriteArrayList();

    public StoreEdgesResponseObserver(CountDownLatch latch, GraphSchema schema, SnapshotMaxGraph graph) {
        this.latch = latch;
        this.graph = graph;
        this.schema = schema;
    }

    @Override
    public void onNext(StoreEdgesResponse storeEdgesResponse) {
        if (0 != storeEdgesResponse.getErrorCode()) {
            throw new RuntimeException(storeEdgesResponse.getErrorMessage());
        }
        for (StoreEdgeResult storeEdgeResult : storeEdgesResponse.getResultsList()) {
            CompositeEdgeId compositeEdgeId = storeEdgeResult.getEdgeId();
            CompositeId srcVertexId = new CompositeId(compositeEdgeId.getSrcId().getId(), compositeEdgeId.getSrcId().getLabelId());
            CompositeId dstVertexId = new CompositeId(compositeEdgeId.getDstId().getId(), compositeEdgeId.getDstId().getLabelId());
            CompositeId edgeId = new CompositeId(compositeEdgeId.getEdgeId().getId(), compositeEdgeId.getEdgeId().getLabelId());
            Vertex srcVertex = new MaxGraphVertex(this.graph, srcVertexId, this.schema.getSchemaElement(srcVertexId.labelId()).getLabel());
            Vertex dstVertex = new MaxGraphVertex(this.graph, dstVertexId, this.schema.getSchemaElement(dstVertexId.labelId()).getLabel());

            Map<String, Object> properties = Maps.newHashMap();
            try {
                StorePropertyListPb propertyListPb = StorePropertyListPb.parseFrom(storeEdgeResult.getProperties());
                for (StorePropertyPb propertyPb : propertyListPb.getPropertiesList()) {
                    int propertyId = propertyPb.getPropertyId();
                    String propertyName = schema.getPropertyDefinition(edgeId.labelId(), propertyId).getName();
                    Object propertyValue = PropertyValue.parseProto(propertyPb.getPropertyValue()).getValue();
                    properties.put(propertyName, propertyValue);
                }
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("Parse property failed in vertex " + storeEdgeResult);
            }
            edgeList.add(new MaxGraphEdge(this.graph, srcVertex, dstVertex, edgeId, this.schema.getSchemaElement(edgeId.labelId()).getLabel(), properties));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("query store edges failed", throwable);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public List<Edge> getEdgeList() {
        return this.edgeList;
    }
}
