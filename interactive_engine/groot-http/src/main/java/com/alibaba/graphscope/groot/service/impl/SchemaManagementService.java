package com.alibaba.graphscope.groot.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.xml.crypto.Data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.EdgeLabel;
import com.alibaba.graphscope.groot.sdk.schema.Property;
import com.alibaba.graphscope.groot.sdk.schema.Schema;
import com.alibaba.graphscope.groot.sdk.schema.VertexLabel;
import com.alibaba.graphscope.groot.service.models.BaseEdgeTypeVertexTypePairRelationsInner;
import com.alibaba.graphscope.groot.service.models.CreateEdgeType;
import com.alibaba.graphscope.groot.service.models.CreateGraphSchemaRequest;
import com.alibaba.graphscope.groot.service.models.CreatePropertyMeta;
import com.alibaba.graphscope.groot.service.models.CreateVertexType;
import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
import com.alibaba.graphscope.proto.GraphDefPb;
import com.alibaba.graphscope.proto.groot.DataTypePb;


@Service
public class SchemaManagementService {
    private final GrootClient grootClient;

    @Autowired
    public SchemaManagementService(GrootClient grootClient) {
        this.grootClient = grootClient;
    }

    public void createVertexType(CreateVertexType createVertexType) {
        VertexLabel vLabelBuilder = createVertexLabel(createVertexType);
        Schema.Builder schema = Schema.newBuilder();
        schema.addVertexLabel(vLabelBuilder);
        grootClient.submitSchema(schema);
    }

    public void deleteVertexType( String vertexType) {
        VertexLabel.Builder vLabelBuilder = VertexLabel.newBuilder().setLabel(vertexType);
        Schema.Builder schema = Schema.newBuilder();
        schema.dropVertexLabel(vLabelBuilder);
        grootClient.submitSchema(schema);
    }

    public void updateVertexType(CreateVertexType updateVertexType) {
        VertexLabel vLabel = createVertexLabel( updateVertexType);
        Schema.Builder schema = Schema.newBuilder();
        schema.addVertexLabelProperties(vLabel);
        grootClient.submitSchema(schema);
    }

    public void createEdgeType( CreateEdgeType createEdgeType) {
        EdgeLabel eLabel = createEdgeLabel( createEdgeType);
        Schema.Builder schema = Schema.newBuilder();
        schema.addEdgeLabel(eLabel);
        grootClient.submitSchema(schema);
    }

    public void deleteEdgeType(String edgeType, String srcVertexType, String dstVertexType) {
        // Delete edge relation, if it is the only relation, then delete edge label
        EdgeLabel.Builder eKindBuilder = EdgeLabel.newBuilder();
        eKindBuilder.setLabel(edgeType);
        eKindBuilder.addRelation(srcVertexType, dstVertexType);
        Schema.Builder schema = Schema.newBuilder();
        schema.dropEdgeLabelOrKind(eKindBuilder);
        GraphDefPb  grootGraphDefPb = grootClient.submitSchema(schema);
        if (grootGraphDefPb.getEdgeKindsList().stream().noneMatch(edgeKind -> edgeKind.getEdgeLabel().equals(edgeType))) {
            // no more edgeKind with the given edge label, so we can delete it.
            EdgeLabel.Builder eLabelBuilder = EdgeLabel.newBuilder();
            eLabelBuilder.setLabel(edgeType);
            Schema.Builder schema2 = Schema.newBuilder();
            schema2.dropEdgeLabelOrKind(eLabelBuilder);
            grootClient.submitSchema(schema2);
        }
    }

    public void updateEdgeType(CreateEdgeType updateEdgeType) {
        EdgeLabel eLabel = createEdgeLabel( updateEdgeType);
        Schema.Builder schema = Schema.newBuilder();
        schema.addEdgeLabelProperties(eLabel);
        grootClient.submitSchema(schema);
    }

    public void importSchema(CreateGraphSchemaRequest createSchema) {
        Schema.Builder schema = Schema.newBuilder();
        for (CreateVertexType vertexType : createSchema.getVertexTypes()) {
            VertexLabel vLabel = createVertexLabel(vertexType);
            schema.addVertexLabel(vLabel);
        }
        for (CreateEdgeType edgeType : createSchema.getEdgeTypes()) {
            EdgeLabel eLabel = createEdgeLabel( edgeType);
            schema.addEdgeLabel(eLabel);
        }
        grootClient.submitSchema(schema);
    }

    public GetGraphSchemaResponse getSchema() {
        Schema schema = Schema.fromGraphDef(grootClient.getSchema());
        GetGraphSchemaResponse response = new GetGraphSchemaResponse();
        for (VertexLabel vertex : schema.getVertexLabels()) {
            response.addVertexTypesItem(DtoConverter.convertToDtoVertexType(vertex));
        }
        for (EdgeLabel edge : schema.getEdgeLabels()) {
            response.addEdgeTypesItem(DtoConverter.convertToDtoEdgeType(edge));
        }
        return response;
    }

    public void dropSchema() {
        grootClient.dropSchema();
    }

    private VertexLabel createVertexLabel(CreateVertexType createVertexType) {
        VertexLabel.Builder vLabelBuilder = VertexLabel.newBuilder();
        vLabelBuilder.setLabel(createVertexType.getTypeName());
        List<String> primaryKeys = createVertexType.getPrimaryKeys();
        for (CreatePropertyMeta prop : createVertexType.getProperties()) {
            DataTypePb dataType = DtoConverter.convertToDataTypePb(prop.getPropertyType());
            Property.Builder property = Property.newBuilder().setName(prop.getPropertyName()).setDataType(dataType);
            if (primaryKeys.contains(prop.getPropertyName())) {
                property.setPrimaryKey();
            }
            vLabelBuilder.addProperty(property);
        }
        return vLabelBuilder.build();
    }

    private EdgeLabel createEdgeLabel(CreateEdgeType createEdgeType) {
        EdgeLabel.Builder eLabelBuilder = EdgeLabel.newBuilder();
        eLabelBuilder.setLabel(createEdgeType.getTypeName());
        for (BaseEdgeTypeVertexTypePairRelationsInner pair : createEdgeType.getVertexTypePairRelations()) {
            eLabelBuilder.addRelation(pair.getSourceVertex(), pair.getDestinationVertex());
        }
        for (CreatePropertyMeta prop : createEdgeType.getProperties()) {
            DataTypePb dataType = DtoConverter.convertToDataTypePb(prop.getPropertyType());
            Property.Builder property = Property.newBuilder().setName(prop.getPropertyName()).setDataType(dataType);
            eLabelBuilder.addProperty(property);
        }
        return eLabelBuilder.build();
    }

    public boolean remoteFlush(long snapshotId) {
        return grootClient.remoteFlush(snapshotId);
    }
}
