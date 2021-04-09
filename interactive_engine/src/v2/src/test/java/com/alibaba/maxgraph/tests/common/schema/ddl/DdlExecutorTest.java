package com.alibaba.maxgraph.tests.common.schema.ddl;

import com.alibaba.maxgraph.proto.v2.DdlRequestBatchPb;
import com.alibaba.maxgraph.proto.v2.GraphDefPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.ddl.AddEdgeKindOperation;
import com.alibaba.maxgraph.v2.common.operation.ddl.CreateEdgeTypeOperation;
import com.alibaba.maxgraph.v2.common.operation.ddl.CreateVertexTypeOperation;
import com.alibaba.maxgraph.v2.common.operation.ddl.DropEdgeTypeOperation;
import com.alibaba.maxgraph.v2.common.operation.ddl.DropVertexTypeOperation;
import com.alibaba.maxgraph.v2.common.operation.ddl.RemoveEdgeKindOperation;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlResult;
import com.alibaba.maxgraph.v2.common.schema.request.AddEdgeKindRequest;
import com.alibaba.maxgraph.v2.common.schema.request.CreateEdgeTypeRequest;
import com.alibaba.maxgraph.v2.common.schema.request.CreateVertexTypeRequest;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import com.alibaba.maxgraph.v2.common.schema.request.DropEdgeTypeRequest;
import com.alibaba.maxgraph.v2.common.schema.request.DropVertexTypeRequest;
import com.alibaba.maxgraph.v2.common.schema.request.RemoveEdgeKindRequest;
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
        PropertyValue defaultValue = new PropertyValue(DataType.INT, ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        PropertyDef propertyDef = new PropertyDef(1, 1, "p1", DataType.INT, defaultValue, true, "property_1");
        TypeDef vertexTypeDef = TypeDef.newBuilder()
                .setLabel("vertex1")
                .addPropertyDef(propertyDef)
                .setTypeEnum(TypeEnum.VERTEX)
                .build();
        TypeDef edgeTypeDef = TypeDef.newBuilder()
                .setLabel("edge1")
                .addPropertyDef(propertyDef)
                .setTypeEnum(TypeEnum.EDGE)
                .build();
        EdgeKind edgeKind = EdgeKind.newBuilder()
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
        assertEquals(DdlRequestBatch.parseProto(DdlRequestBatchPb.parseFrom(bytes)), ddlRequestBatch);

        DdlResult ddlResult = ddlExecutors.executeDdlRequestBatch(ddlRequestBatch, graphDef, 1);
        LabelId vertexLabelId = new LabelId(1);
        LabelId edgeLabelId = new LabelId(2);
        EdgeKind edgeKindWithId = EdgeKind.newBuilder(edgeKind)
                .setEdgeLabelId(edgeLabelId)
                .setSrcVertexLabelId(vertexLabelId)
                .setDstVertexLabelId(vertexLabelId).build();
        TypeDef vertexTypeWithId = TypeDef.newBuilder(vertexTypeDef).setLabelId(vertexLabelId).build();
        TypeDef edgeTypeWithId = TypeDef.newBuilder(edgeTypeDef).setLabelId(edgeLabelId).build();
        graphDef = GraphDef.newBuilder(graphDef)
                .setVersion(3)
                .setLabelIdx(2)
                .setPropertyIdx(1)
                .putPropertyNameToId("p1", 1)
                .addTypeDef(vertexTypeWithId)
                .addTypeDef(edgeTypeWithId)
                .addEdgeKind(edgeKindWithId)
                .build();
        assertEquals(graphDef, ddlResult.getGraphDef());
        List<Operation> ddlOperations = ddlResult.getDdlOperations();
        assertEquals(ddlOperations.size(), 3);
        assertEquals(ddlOperations.get(0).toBlob().toProto(),
                new CreateVertexTypeOperation(0, 1L, vertexTypeWithId, -1L).toBlob().toProto());
        assertEquals(ddlOperations.get(1).toBlob().toProto(),
                new CreateEdgeTypeOperation(0, 2L, edgeTypeWithId).toBlob().toProto());
        assertEquals(ddlOperations.get(2).toBlob().toProto(),
                new AddEdgeKindOperation(0, 3L, edgeKindWithId, -1L).toBlob().toProto());

        assertEquals(GraphDef.parseProto(GraphDefPb.parseFrom(graphDef.toProto().toByteString())), graphDef);

        requestBatchBuilder = DdlRequestBatch.newBuilder();
        requestBatchBuilder.addDdlRequest(new RemoveEdgeKindRequest(edgeKind));
        requestBatchBuilder.addDdlRequest(new DropEdgeTypeRequest(edgeTypeDef.getLabel()));
        requestBatchBuilder.addDdlRequest(new DropVertexTypeRequest(vertexTypeDef.getLabel()));
        ddlResult = ddlExecutors.executeDdlRequestBatch(requestBatchBuilder.build(), graphDef, 1);
        graphDef = GraphDef.newBuilder(graphDef)
                .setVersion(6)
                .removeEdgeKind(edgeKindWithId)
                .removeTypeDef(edgeTypeDef.getLabel())
                .removeTypeDef(vertexTypeDef.getLabel())
                .clearUnusedPropertyName(Collections.emptySet())
                .build();
        assertEquals(graphDef, ddlResult.getGraphDef());
        ddlOperations = ddlResult.getDdlOperations();
        assertEquals(ddlOperations.size(), 3);
        assertEquals(ddlOperations.get(0).toBlob().toProto(),
                new RemoveEdgeKindOperation(0, 4L, edgeKindWithId).toBlob().toProto());
        assertEquals(ddlOperations.get(1).toBlob().toProto(),
                new DropEdgeTypeOperation(0, 5L, edgeLabelId).toBlob().toProto());
        assertEquals(ddlOperations.get(2).toBlob().toProto(),
                new DropVertexTypeOperation(0, 6L, vertexLabelId).toBlob().toProto());

    }
}
