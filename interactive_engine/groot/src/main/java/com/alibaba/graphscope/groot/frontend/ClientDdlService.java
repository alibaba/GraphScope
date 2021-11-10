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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.schema.EdgeKind;
import com.alibaba.graphscope.groot.schema.PropertyDef;
import com.alibaba.graphscope.groot.schema.PropertyValue;
import com.alibaba.graphscope.groot.schema.TypeDef;
import com.alibaba.graphscope.groot.schema.TypeEnum;
import com.alibaba.graphscope.proto.ddl.*;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ClientDdlService extends ClientDdlGrpc.ClientDdlImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ClientDdlService.class);

    public static final int FORMAT_VERSION = 1;

    private SnapshotCache snapshotCache;
    private BatchDdlClient batchDdlClient;

    public ClientDdlService(SnapshotCache snapshotCache, BatchDdlClient batchDdlClient) {
        this.snapshotCache = snapshotCache;
        this.batchDdlClient = batchDdlClient;
    }

    @Override
    public void batchSubmit(
            BatchSubmitRequest request, StreamObserver<BatchSubmitResponse> responseObserver) {
        try {
            int formatVersion = request.getFormatVersion();
            boolean simple = request.getSimpleResponse();
            DdlRequestBatch.Builder builder = DdlRequestBatch.newBuilder();
            for (BatchSubmitRequest.DDLRequest ddlRequest : request.getValueList()) {
                switch (ddlRequest.getValueCase()) {
                    case CREATE_VERTEX_TYPE_REQUEST:
                        CreateVertexTypeRequest cvtReq = ddlRequest.getCreateVertexTypeRequest();
                        builder.addDdlRequest(
                                new com.alibaba.graphscope.groot.schema.request
                                        .CreateVertexTypeRequest(
                                        parseTypeDefPb(cvtReq.getTypeDef())));
                        break;
                    case CREATE_EDGE_TYPE_REQUEST:
                        CreateEdgeTypeRequest cetReq = ddlRequest.getCreateEdgeTypeRequest();
                        builder.addDdlRequest(
                                new com.alibaba.graphscope.groot.schema.request
                                        .CreateEdgeTypeRequest(
                                        parseTypeDefPb(cetReq.getTypeDef())));
                        break;
                    case ADD_EDGE_KIND_REQUEST:
                        AddEdgeKindRequest addEdgeKindRequest = ddlRequest.getAddEdgeKindRequest();
                        builder.addDdlRequest(
                                new com.alibaba.graphscope.groot.schema.request.AddEdgeKindRequest(
                                        EdgeKind.newBuilder()
                                                .setEdgeLabel(addEdgeKindRequest.getEdgeLabel())
                                                .setSrcVertexLabel(
                                                        addEdgeKindRequest.getSrcVertexLabel())
                                                .setDstVertexLabel(
                                                        addEdgeKindRequest.getDstVertexLabel())
                                                .build()));
                        break;
                    case REMOVE_EDGE_KIND_REQUEST:
                        RemoveEdgeKindRequest removeEdgeKindRequest =
                                ddlRequest.getRemoveEdgeKindRequest();
                        builder.addDdlRequest(
                                new com.alibaba.graphscope.groot.schema.request
                                        .RemoveEdgeKindRequest(
                                        EdgeKind.newBuilder()
                                                .setEdgeLabel(removeEdgeKindRequest.getEdgeLabel())
                                                .setSrcVertexLabel(
                                                        removeEdgeKindRequest.getSrcVertexLabel())
                                                .setDstVertexLabel(
                                                        removeEdgeKindRequest.getDstVertexLabel())
                                                .build()));
                        break;
                    case DROP_VERTEX_TYPE_REQUEST:
                        builder.addDdlRequest(
                                new com.alibaba.graphscope.groot.schema.request
                                        .DropVertexTypeRequest(
                                        ddlRequest.getDropVertexTypeRequest().getLabel()));
                        break;
                    case DROP_EDGE_TYPE_REQUEST:
                        builder.addDdlRequest(
                                new com.alibaba.graphscope.groot.schema.request.DropEdgeTypeRequest(
                                        ddlRequest.getDropEdgeTypeRequest().getLabel()));
                        break;
                    case VALUE_NOT_SET:
                        break;
                }
            }
            long snapshotId = batchDdlClient.batchDdl(builder.build());
            this.snapshotCache.addListener(
                    snapshotId,
                    () -> {
                        BatchSubmitResponse.Builder responseBuilder =
                                BatchSubmitResponse.newBuilder();
                        responseBuilder.setFormatVersion(FORMAT_VERSION);
                        if (!simple) {
                            GraphDefPb graphDefPb =
                                    toGraphDefPb(
                                            this.snapshotCache
                                                    .getSnapshotWithSchema()
                                                    .getGraphDef());
                            responseBuilder.setGraphDef(graphDefPb);
                        }
                        responseObserver.onNext(responseBuilder.build());
                        responseObserver.onCompleted();
                    });
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private GraphDefPb toGraphDefPb(GraphDef graphDef) {
        GraphDefPb.Builder builder = GraphDefPb.newBuilder();
        builder.setVersion(graphDef.getVersion());
        builder.setKey("");
        builder.setGraphType(GraphTypePb.PERSISTENT_STORE);
        builder.setDirected(true);
        for (TypeDef typeDef : graphDef.getIdToType().values()) {
            builder.addTypeDefs(toTypeDefPb(typeDef));
        }
        for (Set<EdgeKind> edgeKinds : graphDef.getIdToKinds().values()) {
            for (EdgeKind edgeKind : edgeKinds) {
                EdgeKindPb edgeKindPb =
                        EdgeKindPb.newBuilder()
                                .setEdgeLabel(edgeKind.getEdgeLabel())
                                .setEdgeLabelId(
                                        LabelIdPb.newBuilder()
                                                .setId(edgeKind.getEdgeLabelId().getId()))
                                .setSrcVertexLabel(edgeKind.getSrcVertexLabel())
                                .setSrcVertexLabelId(
                                        LabelIdPb.newBuilder()
                                                .setId(edgeKind.getSrcVertexLabelId().getId()))
                                .setDstVertexLabel(edgeKind.getDstVertexLabel())
                                .setDstVertexLabelId(
                                        LabelIdPb.newBuilder()
                                                .setId(edgeKind.getSrcVertexLabelId().getId()))
                                .build();
                builder.addEdgeKinds(edgeKindPb);
            }
        }
        MaxGraphInfoPb maxGraphInfoPb =
                MaxGraphInfoPb.newBuilder()
                        .setLastLabelId(graphDef.getLabelIdx())
                        .setLastTableId(graphDef.getTableIdx())
                        .setLastPropertyId(graphDef.getPropertyIdx())
                        .build();
        builder.setExtension(Any.pack(maxGraphInfoPb));
        return builder.build();
    }

    private TypeDefPb toTypeDefPb(TypeDef typeDef) {
        TypeDefPb.Builder builder = TypeDefPb.newBuilder();
        builder.setVersionId(typeDef.getVersionId());
        builder.setLabel(typeDef.getLabel());
        builder.setLabelId(LabelIdPb.newBuilder().setId(typeDef.getLabelId()).build());
        switch (typeDef.getTypeEnum()) {
            case VERTEX:
                builder.setTypeEnum(TypeEnumPb.VERTEX);
                break;
            case EDGE:
                builder.setTypeEnum(TypeEnumPb.EDGE);
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid type enum [" + typeDef.getTypeEnum() + "]");
        }
        for (PropertyDef property : typeDef.getProperties()) {
            builder.addProps(toPropertyDefPb(property));
        }
        return builder.build();
    }

    private PropertyDefPb toPropertyDefPb(PropertyDef propertyDef) {
        PropertyDefPb.Builder builder = PropertyDefPb.newBuilder();
        builder.setId(propertyDef.getId());
        builder.setInnerId(propertyDef.getId());
        builder.setName(propertyDef.getName());
        builder.setDataType(DataTypePb.valueOf(propertyDef.getDataType().name()));
        builder.setPk(propertyDef.isPartOfPrimaryKey());
        builder.setComment(propertyDef.getComment());
        PropertyValue propertyValue = propertyDef.getDefaultPropertyValue();
        builder.setDefaultValue(
                PropertyValuePb.newBuilder()
                        .setDataType(DataTypePb.valueOf(propertyValue.getDataType().name()))
                        .setVal(ByteString.copyFrom(propertyValue.getValBytes()))
                        .build());
        return builder.build();
    }

    private TypeDef parseTypeDefPb(TypeDefPb typeDefPb) {
        TypeDef.Builder builder = TypeDef.newBuilder();
        builder.setLabel(typeDefPb.getLabel());
        TypeEnumPb typeEnumPb = typeDefPb.getTypeEnum();
        switch (typeEnumPb) {
            case VERTEX:
                builder.setTypeEnum(TypeEnum.VERTEX);
                break;
            case EDGE:
                builder.setTypeEnum(TypeEnum.EDGE);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + typeEnumPb);
        }
        for (PropertyDefPb propertyDefPb : typeDefPb.getPropsList()) {
            PropertyDef.Builder propBuilder = PropertyDef.newBuilder();
            propBuilder.setName(propertyDefPb.getName());
            propBuilder.setPk(propertyDefPb.getPk());
            propBuilder.setComment(propertyDefPb.getComment());
            propBuilder.setDataType(parseDataType(propertyDefPb.getDataType()));
            PropertyValuePb defaultValuePb = propertyDefPb.getDefaultValue();
            propBuilder.setDefaultValue(
                    new PropertyValue(
                            parseDataType(defaultValuePb.getDataType()),
                            defaultValuePb.getVal().toByteArray()));
            builder.addPropertyDef(propBuilder.build());
        }
        return builder.build();
    }

    private DataType parseDataType(DataTypePb dataTypePb) {
        return DataType.fromId((byte) dataTypePb.getNumber());
    }

    @Override
    public void getGraphDef(
            GetGraphDefRequest request, StreamObserver<GetGraphDefResponse> responseObserver) {
        GraphDefPb graphDefPb =
                toGraphDefPb(this.snapshotCache.getSnapshotWithSchema().getGraphDef());
        responseObserver.onNext(GetGraphDefResponse.newBuilder().setGraphDef(graphDefPb).build());
        responseObserver.onCompleted();
    }
}
