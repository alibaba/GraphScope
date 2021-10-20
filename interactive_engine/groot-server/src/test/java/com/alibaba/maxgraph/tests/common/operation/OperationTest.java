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
package com.alibaba.maxgraph.tests.common.operation;

import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.proto.groot.DataOperationPb;
import com.alibaba.maxgraph.proto.groot.DdlOperationPb;
import com.alibaba.maxgraph.proto.groot.EdgeKindPb;
import com.alibaba.maxgraph.proto.groot.LabelIdPb;
import com.alibaba.maxgraph.proto.groot.OpTypePb;
import com.alibaba.maxgraph.proto.groot.OperationPb;
import com.alibaba.maxgraph.proto.groot.PropertyValuePb;
import com.alibaba.maxgraph.proto.groot.TypeDefPb;
import com.alibaba.graphscope.groot.operation.EdgeId;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.VertexId;
import com.alibaba.graphscope.groot.operation.ddl.AddEdgeKindOperation;
import com.alibaba.graphscope.groot.operation.ddl.CreateEdgeTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.CreateVertexTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.DropEdgeTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.DropVertexTypeOperation;
import com.alibaba.graphscope.groot.operation.ddl.RemoveEdgeKindOperation;
import com.alibaba.graphscope.groot.operation.dml.DeleteEdgeOperation;
import com.alibaba.graphscope.groot.operation.dml.DeleteVertexOperation;
import com.alibaba.graphscope.groot.operation.dml.OverwriteEdgeOperation;
import com.alibaba.graphscope.groot.operation.dml.OverwriteVertexOperation;
import com.alibaba.graphscope.groot.operation.dml.UpdateEdgeOperation;
import com.alibaba.graphscope.groot.operation.dml.UpdateVertexOperation;
import com.alibaba.graphscope.groot.schema.EdgeKind;
import com.alibaba.graphscope.groot.schema.PropertyDef;
import com.alibaba.graphscope.groot.schema.PropertyValue;
import com.alibaba.graphscope.groot.schema.TypeDef;
import com.alibaba.graphscope.groot.schema.TypeEnum;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OperationTest {

    EdgeKind testEdgeKind =
            EdgeKind.newBuilder()
                    .setEdgeLabel("test_edge")
                    .setEdgeLabelId(new LabelId(1))
                    .setDstVertexLabel("dst_v")
                    .setDstVertexLabelId(new LabelId(2))
                    .setSrcVertexLabel("src_v")
                    .setSrcVertexLabelId(new LabelId(3))
                    .build();

    PropertyDef testPropertyDef =
            PropertyDef.newBuilder()
                    .setId(0)
                    .setInnerId(0)
                    .setName("test_prop")
                    .setDataType(DataType.STRING)
                    .setDefaultValue(new PropertyValue(DataType.STRING, "default_val"))
                    .setPk(true)
                    .setComment("comment")
                    .build();

    TypeDef testTypeDef =
            TypeDef.newBuilder()
                    .setLabel("test_type_def")
                    .setLabelId(new LabelId(10))
                    .setTypeEnum(TypeEnum.VERTEX)
                    .addPropertyDef(testPropertyDef)
                    .build();

    @Test
    void testSerde() throws InvalidProtocolBufferException {
        ByteString testEdgeBytes = testEdgeKind.toProto().toByteString();
        EdgeKind edgeKind = EdgeKind.parseProto(EdgeKindPb.parseFrom(testEdgeBytes));
        assertEquals(testEdgeKind, edgeKind);

        ByteString testTypeDefBytes = testTypeDef.toProto().toByteString();
        TypeDef typeDef = TypeDef.parseProto(TypeDefPb.parseFrom(testTypeDefBytes));
        assertEquals(testTypeDef, typeDef);
    }

    @Test
    void testDdlOperations() throws InvalidProtocolBufferException {
        OperationPb operationPb =
                new AddEdgeKindOperation(1, 10L, testEdgeKind, -1).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.ADD_EDGE_KIND);
        DdlOperationPb ddlOperationPb = DdlOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(ddlOperationPb.getSchemaVersion(), 10L);
        ByteString addEdgeKindBytes = ddlOperationPb.getDdlBlob();
        assertEquals(EdgeKind.parseProto(EdgeKindPb.parseFrom(addEdgeKindBytes)), testEdgeKind);

        operationPb = new RemoveEdgeKindOperation(1, 10L, testEdgeKind).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.REMOVE_EDGE_KIND);
        ddlOperationPb = DdlOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(ddlOperationPb.getSchemaVersion(), 10L);
        ByteString removeEdgeKindBytes = ddlOperationPb.getDdlBlob();
        assertEquals(EdgeKind.parseProto(EdgeKindPb.parseFrom(removeEdgeKindBytes)), testEdgeKind);

        operationPb = new CreateEdgeTypeOperation(1, 10L, testTypeDef).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.CREATE_EDGE_TYPE);
        ddlOperationPb = DdlOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(ddlOperationPb.getSchemaVersion(), 10L);
        ByteString edgeTypeDefBytes = ddlOperationPb.getDdlBlob();
        assertEquals(TypeDef.parseProto(TypeDefPb.parseFrom(edgeTypeDefBytes)), testTypeDef);

        operationPb = new CreateVertexTypeOperation(1, 10L, testTypeDef, -1).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.CREATE_VERTEX_TYPE);
        ddlOperationPb = DdlOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(ddlOperationPb.getSchemaVersion(), 10L);
        ByteString vertexTypeDefBytes = ddlOperationPb.getDdlBlob();
        assertEquals(TypeDef.parseProto(TypeDefPb.parseFrom(vertexTypeDefBytes)), testTypeDef);

        operationPb = new DropEdgeTypeOperation(1, 10L, new LabelId(1)).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.DROP_EDGE_TYPE);
        ddlOperationPb = DdlOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(ddlOperationPb.getSchemaVersion(), 10L);
        assertEquals(LabelIdPb.parseFrom(ddlOperationPb.getDdlBlob()).getId(), 1);

        operationPb = new DropVertexTypeOperation(1, 10L, new LabelId(1)).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.DROP_VERTEX_TYPE);
        ddlOperationPb = DdlOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(ddlOperationPb.getSchemaVersion(), 10L);
        assertEquals(LabelIdPb.parseFrom(ddlOperationPb.getDdlBlob()).getId(), 1);
    }

    @Test
    void testDmlOperations() throws InvalidProtocolBufferException {
        VertexId srcVertexId = new VertexId(1L);
        VertexId dstVertexId = new VertexId(2L);
        EdgeId edgeId = new EdgeId(srcVertexId, dstVertexId, 3L);

        OperationPb operationPb =
                new DeleteEdgeOperation(edgeId, testEdgeKind, true).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.DELETE_EDGE);
        DataOperationPb dataOperationPb = DataOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(dataOperationPb.getKeyBlob(), edgeId.toProto().toByteString());
        assertEquals(
                dataOperationPb.getLocationBlob(), testEdgeKind.toOperationProto().toByteString());

        LabelId labelId = new LabelId(1);
        operationPb = new DeleteVertexOperation(srcVertexId, labelId).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.DELETE_VERTEX);
        dataOperationPb = DataOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(dataOperationPb.getKeyBlob(), srcVertexId.toProto().toByteString());
        assertEquals(dataOperationPb.getLocationBlob(), labelId.toProto().toByteString());

        PropertyValue propertyValue = new PropertyValue(DataType.STRING, "val");
        Map<Integer, PropertyValue> properties = Collections.singletonMap(1, propertyValue);

        operationPb =
                new OverwriteEdgeOperation(edgeId, testEdgeKind, properties, true)
                        .toBlob()
                        .toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.OVERWRITE_EDGE);
        dataOperationPb = DataOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(dataOperationPb.getKeyBlob(), edgeId.toProto().toByteString());
        assertEquals(
                dataOperationPb.getLocationBlob(), testEdgeKind.toOperationProto().toByteString());
        Map<Integer, PropertyValuePb> propsMap = dataOperationPb.getPropsMap();
        assertEquals(propsMap.size(), 1);
        assertEquals(propsMap.get(1), propertyValue.toProto());

        operationPb =
                new UpdateEdgeOperation(edgeId, testEdgeKind, properties, true).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.UPDATE_EDGE);
        dataOperationPb = DataOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(dataOperationPb.getKeyBlob(), edgeId.toProto().toByteString());
        assertEquals(
                dataOperationPb.getLocationBlob(), testEdgeKind.toOperationProto().toByteString());
        propsMap = dataOperationPb.getPropsMap();
        assertEquals(propsMap.size(), 1);
        assertEquals(propsMap.get(1), propertyValue.toProto());

        operationPb =
                new OverwriteVertexOperation(srcVertexId, labelId, properties).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.OVERWRITE_VERTEX);
        dataOperationPb = DataOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(dataOperationPb.getKeyBlob(), srcVertexId.toProto().toByteString());
        assertEquals(dataOperationPb.getLocationBlob(), labelId.toProto().toByteString());
        propsMap = dataOperationPb.getPropsMap();
        assertEquals(propsMap.size(), 1);
        assertEquals(propsMap.get(1), propertyValue.toProto());

        operationPb =
                new UpdateVertexOperation(srcVertexId, labelId, properties).toBlob().toProto();
        assertEquals(operationPb.getPartitionKey(), 1L);
        assertEquals(operationPb.getOpType(), OpTypePb.UPDATE_VERTEX);
        dataOperationPb = DataOperationPb.parseFrom(operationPb.getDataBytes());
        assertEquals(dataOperationPb.getKeyBlob(), srcVertexId.toProto().toByteString());
        assertEquals(dataOperationPb.getLocationBlob(), labelId.toProto().toByteString());
        propsMap = dataOperationPb.getPropsMap();
        assertEquals(propsMap.size(), 1);
        assertEquals(propsMap.get(1), propertyValue.toProto());
    }
}
