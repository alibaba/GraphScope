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

    public void createVertexType(String graphId, CreateVertexType createVertexType) {
        VertexLabel vLabelBuilder = createVertexLabel(graphId, createVertexType);
        System.out.println("vLabelBuilder: ");
        System.out.println(vLabelBuilder);
        Schema.Builder schema = Schema.newBuilder();
        schema.addVertexLabel(vLabelBuilder);
        GraphDefPb res = grootClient.submitSchema(schema);
        System.out.println("res: " + res);
    }

    public void deleteVertexType(String graphId, String vertexType) {
        VertexLabel.Builder vLabelBuilder = VertexLabel.newBuilder();
        vLabelBuilder.setLabel(vertexType);
        Schema.Builder schema = Schema.newBuilder();
        schema.dropVertexLabel(vLabelBuilder);
        grootClient.submitSchema(schema);
    }

    public void createEdgeType(String graphId, CreateEdgeType createEdgeType) {
        EdgeLabel eLabelBuilder = createEdgeLabel(graphId, createEdgeType);
        Schema.Builder schema = Schema.newBuilder();
        schema.addEdgeLabel(eLabelBuilder);
        grootClient.submitSchema(schema);
    }

    public void deleteEdgeType(String graphId, String edgeType, String srcVertexType, String dstVertexType) {
        EdgeLabel.Builder eLabelBuilder = EdgeLabel.newBuilder();
        eLabelBuilder.setLabel(edgeType);
        eLabelBuilder.addRelation(srcVertexType, dstVertexType);
        Schema.Builder schema = Schema.newBuilder();
        schema.dropEdgeLabelOrKind(eLabelBuilder);
        grootClient.submitSchema(schema);
    }

    public void importSchema(String graphId, CreateGraphSchemaRequest schema) {
        Schema.Builder grootSchema = Schema.newBuilder();
        for (CreateVertexType vertexType : schema.getVertexTypes()) {
            VertexLabel vLabelBuilder = createVertexLabel(graphId, vertexType);
            grootSchema.addVertexLabel(vLabelBuilder);
        }
        for (CreateEdgeType edgeType : schema.getEdgeTypes()) {
            EdgeLabel eLabelBuilder = createEdgeLabel(graphId, edgeType);
            grootSchema.addEdgeLabel(eLabelBuilder);
        }
        grootClient.submitSchema(grootSchema);
    }

    public GetGraphSchemaResponse getSchema(String graphId) {
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

    public void dropSchema(String graphId) {
        // TODO: seems not working
        grootClient.dropSchema();
    }

    private VertexLabel createVertexLabel(String graphId, CreateVertexType createVertexType) {
        VertexLabel.Builder vLabelBuilder = VertexLabel.newBuilder();
        vLabelBuilder.setLabel(createVertexType.getTypeName());
        for (String pk : createVertexType.getPrimaryKeys()) {
            // TODO: set datatype for primary key
            Property.Builder property = Property.newBuilder().setName(pk).setDataType(DataTypePb.STRING).setPrimaryKey();
            vLabelBuilder.addProperty(property);
        }
        for (CreatePropertyMeta prop : createVertexType.getProperties()) {
            DataTypePb dataType = DtoConverter.convertToDataTypePb(prop.getPropertyType());
            Property.Builder property = Property.newBuilder().setName(prop.getPropertyName()).setDataType(dataType);
            vLabelBuilder.addProperty(property);
        }
        return vLabelBuilder.build();
    }

    private EdgeLabel createEdgeLabel(String graphId, CreateEdgeType createEdgeType) {
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
}
