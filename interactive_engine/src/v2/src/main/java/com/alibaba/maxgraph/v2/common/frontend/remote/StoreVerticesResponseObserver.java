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

import com.alibaba.maxgraph.proto.v2.StorePropertyListPb;
import com.alibaba.maxgraph.proto.v2.StorePropertyPb;
import com.alibaba.maxgraph.proto.v2.StoreVertexResult;
import com.alibaba.maxgraph.proto.v2.StoreVerticesResponse;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphVertex;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class StoreVerticesResponseObserver implements StreamObserver<StoreVerticesResponse> {
    private static final Logger logger = LoggerFactory.getLogger(StoreVerticesResponseObserver.class);

    private CountDownLatch latch;
    private List<Vertex> vertexList;
    private GraphSchema schema;
    private SnapshotMaxGraph graph;

    public StoreVerticesResponseObserver(CountDownLatch latch,
                                         GraphSchema schema,
                                         SnapshotMaxGraph graph) {
        this.latch = latch;
        this.vertexList = Lists.newCopyOnWriteArrayList();
        this.schema = schema;
        this.graph = graph;
    }

    @Override
    public void onNext(StoreVerticesResponse storeVerticesResponse) {
        if (0 == storeVerticesResponse.getErrorCode()) {
            for (StoreVertexResult storeVertexResult : storeVerticesResponse.getResultsList()) {
                CompositeId id = new CompositeId(storeVertexResult.getVertexId().getId(), storeVertexResult.getVertexId().getLabelId());
                SchemaElement schemaElement = schema.getSchemaElement(id.labelId());
                String label = schemaElement.getLabel();
                Map<String, Object> properties = Maps.newHashMap();
                try {
                    StorePropertyListPb propertyListPb = StorePropertyListPb.parseFrom(storeVertexResult.getProperties());
                    for (StorePropertyPb propertyPb : propertyListPb.getPropertiesList()) {
                        int propertyId = propertyPb.getPropertyId();
                        GraphProperty graphProperty = schema.getPropertyDefinition(id.labelId(), propertyId);
                        if (null == graphProperty) {
                            throw new RuntimeException("Cant get property for label=" + id.labelId() + " property id=" + propertyId);
                        }
                        String propertyName = graphProperty.getName();
                        Object propertyValue = PropertyValue.parseProto(propertyPb.getPropertyValue()).getValue();
                        properties.put(propertyName, propertyValue);
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Parse property failed in vertex " + storeVertexResult);
                }
                vertexList.add(new MaxGraphVertex(graph, id, label, properties));
            }
        } else {
            logger.error(storeVerticesResponse.getErrorMessage());
            throw new RuntimeException(storeVerticesResponse.toString());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("query store vertices failed", throwable);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    List<Vertex> getVertexList() {
        return this.vertexList;
    }
}
