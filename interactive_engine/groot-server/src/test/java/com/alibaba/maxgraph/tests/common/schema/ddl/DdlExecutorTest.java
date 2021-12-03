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
package com.alibaba.maxgraph.tests.common.schema.ddl;

import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.proto.groot.DdlRequestBatchPb;
import com.alibaba.maxgraph.proto.groot.GraphDefPb;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.ddl.AddEdgeKindOperation;
import com.alibaba.graphscope.groot.operation.ddl.CreateEdgeTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.CreateVertexTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.DropEdgeTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.DropVertexTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.RemoveEdgeKindOperation;
import com.alibaba.graphscope.groot.schema.EdgeKind;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.PropertyDef;
import com.alibaba.graphscope.groot.schema.PropertyValue;
import com.alibaba.graphscope.groot.schema.TypeDef;
import com.alibaba.graphscope.groot.schema.TypeEnum;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.graphscope.groot.schema.ddl.DdlResult;
import com.alibaba.graphscope.groot.schema.request.AddEdgeKindRequest;
import com.alibaba.graphscope.groot.schema.request.CreateEdgeTypeRequest;
import com.alibaba.graphscope.groot.schema.request.CreateVertexTypeRequest;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.alibaba.graphscope.groot.schema.request.DropEdgeTypeRequest;
import com.alibaba.graphscope.groot.schema.request.DropVertexTypeRequest;
import com.alibaba.graphscope.groot.schema.request.RemoveEdgeKindRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DdlExecutorTest {

    @Test
    void testExecutor() throws InvalidProtocolBufferException {
        PropertyValue defaultValue =
                new PropertyValue(
                        DataType.INT, ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        PropertyDef propertyDef =
                new PropertyDef(1, 1, "p1", DataType.INT, defaultValue, true, "property_1");
        TypeDef vertexTypeDef =
                TypeDef.newBuilder()
                        .setLabel("vertex1")
                        .addPropertyDef(propertyDef)
                        .setTypeEnum(TypeEnum.VERTEX)
                        .build();
        TypeDef edgeTypeDef =
                TypeDef.newBuilder()
                        .setLabel("edge1")
                        .addPropertyDef(propertyDef)
                        .setTypeEnum(TypeEnum.EDGE)
                        .build();
        EdgeKind edgeKind =
                EdgeKind.newBuilder()
                        .setEdgeLabel("edge1")
                        .setDstVertexLabel("vertex1")
                        .setSrcVertexLabel("vertex1")
                        .build();

        GraphDef graphDef = GraphDef.newBuilder().build();

        DdlExecutors ddlExecutors = new DdlExecutors();
        DdlRequestBatch.Builder requestBatchBuilder = DdlRequestBatch.newBuilder();
        requestBatchBuilder.addDdlRequest(new CreateVertexTypeRequest(vertexTypeDef));
        requestBatchBuilder.addDdlRequest(new CreateEdgeTypeRequest(edgeTypeDef));
        requestBatchBuilder.addDdlRequest(new AddEdgeKindRequest(edgeKind));
        DdlRequestBatch ddlRequestBatch = requestBatchBuilder.build();

        // Test serde
        ByteString bytes = ddlRequestBatch.toProto().toByteString();
        assertEquals(
                DdlRequestBatch.parseProto(DdlRequestBatchPb.parseFrom(bytes)), ddlRequestBatch);

        DdlResult ddlResult = ddlExecutors.executeDdlRequestBatch(ddlRequestBatch, graphDef, 1);
        LabelId vertexLabelId = new LabelId(1);
        LabelId edgeLabelId = new LabelId(2);
        EdgeKind edgeKindWithId =
                EdgeKind.newBuilder(edgeKind)
                        .setEdgeLabelId(edgeLabelId)
                        .setSrcVertexLabelId(vertexLabelId)
                        .setDstVertexLabelId(vertexLabelId)
                        .build();
        TypeDef vertexTypeWithId =
                TypeDef.newBuilder(vertexTypeDef).setLabelId(vertexLabelId).build();
        TypeDef edgeTypeWithId = TypeDef.newBuilder(edgeTypeDef).setLabelId(edgeLabelId).build();
        graphDef =
                GraphDef.newBuilder(graphDef)
                        .setVersion(3)
                        .setLabelIdx(2)
                        .setPropertyIdx(1)
                        .putPropertyNameToId("p1", 1)
                        .addTypeDef(vertexTypeWithId)
                        .putVertexTableId(vertexTypeWithId.getTypeLabelId(), 1L)
                        .addTypeDef(edgeTypeWithId)
                        .addEdgeKind(edgeKindWithId)
                        .putEdgeTableId(edgeKindWithId, 2L)
                        .setTableIdx(2L)
                        .build();
        assertEquals(graphDef, ddlResult.getGraphDef());
        List<Operation> ddlOperations = ddlResult.getDdlOperations();
        assertEquals(ddlOperations.size(), 3);
        assertEquals(
                ddlOperations.get(0).toBlob().toProto(),
                new CreateVertexTypeOperation(0, 1L, vertexTypeWithId, 1L).toBlob().toProto());
        assertEquals(
                ddlOperations.get(1).toBlob().toProto(),
                new CreateEdgeTypeOperation(0, 2L, edgeTypeWithId).toBlob().toProto());
        assertEquals(
                ddlOperations.get(2).toBlob().toProto(),
                new AddEdgeKindOperation(0, 3L, edgeKindWithId, 2L).toBlob().toProto());

        assertEquals(
                GraphDef.parseProto(GraphDefPb.parseFrom(graphDef.toProto().toByteString())),
                graphDef);

        requestBatchBuilder = DdlRequestBatch.newBuilder();
        requestBatchBuilder.addDdlRequest(new RemoveEdgeKindRequest(edgeKind));
        requestBatchBuilder.addDdlRequest(new DropEdgeTypeRequest(edgeTypeDef.getLabel()));
        requestBatchBuilder.addDdlRequest(new DropVertexTypeRequest(vertexTypeDef.getLabel()));
        ddlResult = ddlExecutors.executeDdlRequestBatch(requestBatchBuilder.build(), graphDef, 1);
        graphDef =
                GraphDef.newBuilder(graphDef)
                        .setVersion(6)
                        .removeEdgeKind(edgeKindWithId)
                        .removeTypeDef(edgeTypeDef.getLabel())
                        .removeTypeDef(vertexTypeDef.getLabel())
                        .clearUnusedPropertyName(Collections.emptySet())
                        .build();
        assertEquals(graphDef, ddlResult.getGraphDef());
        ddlOperations = ddlResult.getDdlOperations();
        assertEquals(ddlOperations.size(), 3);
        assertEquals(
                ddlOperations.get(0).toBlob().toProto(),
                new RemoveEdgeKindOperation(0, 4L, edgeKindWithId).toBlob().toProto());
        assertEquals(
                ddlOperations.get(1).toBlob().toProto(),
                new DropEdgeTypeOperation(0, 5L, edgeLabelId).toBlob().toProto());
        assertEquals(
                ddlOperations.get(2).toBlob().toProto(),
                new DropVertexTypeOperation(0, 6L, vertexLabelId).toBlob().toProto());
    }
}
