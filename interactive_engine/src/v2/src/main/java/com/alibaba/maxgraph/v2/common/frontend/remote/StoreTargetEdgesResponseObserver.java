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
package com.alibaba.maxgraph.v2.common.frontend.remote;

import com.alibaba.maxgraph.proto.v2.CompositeEdgeId;
import com.alibaba.maxgraph.proto.v2.StoreEdgeResult;
import com.alibaba.maxgraph.proto.v2.StoreEdgesResponse;
import com.alibaba.maxgraph.proto.v2.StorePropertyListPb;
import com.alibaba.maxgraph.proto.v2.StorePropertyPb;
import com.alibaba.maxgraph.proto.v2.StoreTargetEdgeResult;
import com.alibaba.maxgraph.proto.v2.StoreTargetEdgesResponse;
import com.alibaba.maxgraph.proto.v2.StoreTargetEdgesResult;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class StoreTargetEdgesResponseObserver implements StreamObserver<StoreTargetEdgesResponse> {
    private static final Logger logger = LoggerFactory.getLogger(StoreTargetEdgesResponseObserver.class);

    private CountDownLatch latch;
    private SnapshotMaxGraph graph;
    private GraphSchema schema;
    private Direction direction;
    private ElementId vertexId;
    private List<Edge> edgeList = Lists.newCopyOnWriteArrayList();

    public StoreTargetEdgesResponseObserver(Direction direction,
                                            ElementId vertexId,
                                            CountDownLatch latch,
                                            GraphSchema schema,
                                            SnapshotMaxGraph graph) {
        this.direction = direction;
        this.vertexId = vertexId;
        this.latch = latch;
        this.graph = graph;
        this.schema = schema;
    }

    @Override
    public void onNext(StoreTargetEdgesResponse storeEdgesResponse) {
        if (0 != storeEdgesResponse.getErrorCode()) {
            throw new RuntimeException(storeEdgesResponse.getErrorMessage());
        }
        for (StoreTargetEdgesResult storeEdgesResult : storeEdgesResponse.getResultsList()) {
            for (StoreTargetEdgeResult storeTargetEdgeResult : storeEdgesResult.getTargetEdgesList()) {
                CompositeId edgeId = new CompositeId(storeTargetEdgeResult.getEdgeId().getId(), storeTargetEdgeResult.getEdgeId().getLabelId());
                CompositeId targetElementId = new CompositeId(storeTargetEdgeResult.getTargetId().getId(), storeTargetEdgeResult.getTargetId().getLabelId());
                Vertex targetVertex = new MaxGraphVertex(this.graph, targetElementId, this.schema.getSchemaElement(targetElementId.labelId()).getLabel());
                Vertex inputVertex = new MaxGraphVertex(this.graph, vertexId, this.schema.getSchemaElement(vertexId.labelId()).getLabel());
                Map<String, Object> properties = Maps.newHashMap();
                try {
                    StorePropertyListPb propertyListPb = StorePropertyListPb.parseFrom(storeTargetEdgeResult.getProperties());
                    for (StorePropertyPb propertyPb : propertyListPb.getPropertiesList()) {
                        int propertyId = propertyPb.getPropertyId();
                        String propertyName = schema.getPropertyDefinition(edgeId.labelId(), propertyId).getName();
                        Object propertyValue = PropertyValue.parseProto(propertyPb.getPropertyValue()).getValue();
                        properties.put(propertyName, propertyValue);
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Parse property failed in vertex " + storeTargetEdgeResult);
                }
                if (direction == Direction.OUT) {
                    edgeList.add(new MaxGraphEdge(this.graph, inputVertex, targetVertex, edgeId, this.schema.getSchemaElement(edgeId.labelId()).getLabel(), properties));
                } else {
                    edgeList.add(new MaxGraphEdge(this.graph, targetVertex, inputVertex, edgeId, this.schema.getSchemaElement(edgeId.labelId()).getLabel(), properties));
                }
            }
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
