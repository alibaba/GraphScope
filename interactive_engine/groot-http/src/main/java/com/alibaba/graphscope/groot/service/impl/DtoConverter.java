package com.alibaba.graphscope.groot.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.openapitools.jackson.nullable.JsonNullable;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.EdgeLabel;
import com.alibaba.graphscope.groot.sdk.schema.EdgeLabel.EdgeRelation;
import com.alibaba.graphscope.groot.sdk.schema.Schema;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.sdk.schema.VertexLabel;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.GSDataType;
import com.alibaba.graphscope.groot.service.models.GetEdgeType;
import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
import com.alibaba.graphscope.groot.service.models.GetPropertyMeta;
import com.alibaba.graphscope.groot.service.models.GetVertexType;
import com.alibaba.graphscope.groot.service.models.PrimitiveType;
import com.alibaba.graphscope.groot.service.models.Property;
import com.alibaba.graphscope.groot.service.models.StringType;
import com.alibaba.graphscope.groot.service.models.TemporalType;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.alibaba.graphscope.groot.service.models.TimeStampType;
import com.alibaba.graphscope.groot.service.models.VertexRequest;
import com.alibaba.graphscope.groot.service.models.BaseEdgeTypeVertexTypePairRelationsInner;
import com.alibaba.graphscope.groot.service.models.DateType;
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.DeleteVertexRequest;
import com.alibaba.graphscope.proto.groot.DataTypePb;
import com.alibaba.graphscope.proto.groot.GraphDefPb;

public class DtoConverter {
    public static Vertex convertToVertex(VertexRequest vertexRequest) {
        String label = vertexRequest.getLabel();
        Map<String, String> propertyMap = convertToPropertyMap(vertexRequest.getProperties());
        Map<String, String> primaryKeyValues = convertToPropertyMap(vertexRequest.getPrimaryKeyValues());
        propertyMap.putAll(primaryKeyValues);
        return new Vertex(label, propertyMap);
    }

    public static Vertex convertToVertex(DeleteVertexRequest deleteVertexRequest) {
        String label = deleteVertexRequest.getLabel();
        Map<String, String> primaryKeyValues = convertToPropertyMap(deleteVertexRequest.getPrimaryKeyValues());
        return new Vertex(label, primaryKeyValues);
    }

    public static Edge convertToEdge(EdgeRequest edgeRequest) {
        String label = edgeRequest.getEdgeLabel();
        String srcLabel = edgeRequest.getSrcLabel();
        String dstLabel = edgeRequest.getDstLabel();
        Map<String, String> srcPkMap = convertToPropertyMap(edgeRequest.getSrcPrimaryKeyValues());
        Map<String, String> dstPkMap = convertToPropertyMap(edgeRequest.getDstPrimaryKeyValues());
        Map<String, String> propertyMap = convertToPropertyMap(edgeRequest.getProperties());
        return new Edge(label, srcLabel, dstLabel, srcPkMap, dstPkMap, propertyMap);
    }

    public static Edge convertToEdge(DeleteEdgeRequest deleteEdgeRequest) {
        String label = deleteEdgeRequest.getEdgeLabel();
        String srcLabel = deleteEdgeRequest.getSrcLabel();
        String dstLabel = deleteEdgeRequest.getDstLabel();
        Map<String, String> srcPkMap = convertToPropertyMap(deleteEdgeRequest.getSrcPrimaryKeyValues());
        Map<String, String> dstPkMap = convertToPropertyMap(deleteEdgeRequest.getDstPrimaryKeyValues());
        return new Edge(label, srcLabel, dstLabel, srcPkMap, dstPkMap);
    }

    public static DataTypePb convertToDataTypePb(GSDataType dataType) {
        if (dataType instanceof PrimitiveType) {
            PrimitiveType primitiveType = (PrimitiveType) dataType;
            switch (primitiveType.getPrimitiveType()) {
                case SIGNED_INT32:
                    return DataTypePb.INT;
                case UNSIGNED_INT32:
                    return DataTypePb.UINT;
                case SIGNED_INT64:
                    return DataTypePb.LONG;
                case UNSIGNED_INT64:
                    return DataTypePb.ULONG;
                case BOOL:
                    return DataTypePb.BOOL;
                case FLOAT:
                    return DataTypePb.FLOAT;
                case DOUBLE:
                    return DataTypePb.DOUBLE;
                case STRING:
                    return DataTypePb.STRING;
                default:
                    throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType);
            }
        } else if (dataType instanceof StringType) {
            return DataTypePb.STRING;
        } else if (dataType instanceof TemporalType) {
            TemporalType temporalType = (TemporalType) dataType;
            TemporalTypeTemporal temporal = temporalType.getTemporal();
            if (temporal instanceof DateType) {
                // TODO: confirm the date type
                return DataTypePb.DATE32;
            } else if (temporal instanceof TimeStampType) {
                // TODO: confirm the timestamp type
                return DataTypePb.TIMESTAMP_S;
            } else {
                throw new IllegalArgumentException("Unsupported temporal type: " + temporalType);
            }
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    public static GetVertexType convertToDtoVertexType(VertexLabel vertexLabel) {
        GetVertexType vertexType = new GetVertexType();
        vertexType.setTypeName(vertexLabel.getLabel());
        vertexType.setTypeId(vertexLabel.getId());
        vertexType.setProperties(convertToDtoProperties(vertexLabel.getProperties()));
        vertexType.setPrimaryKeys(vertexLabel.getProperties().stream().filter(p -> p.isPrimaryKey()).map(p -> p.getName()).collect(Collectors.toList()));
        return vertexType;
    }

    public static GetEdgeType convertToDtoEdgeType(EdgeLabel edgeLabel) {
        GetEdgeType edgeType = new GetEdgeType();
        edgeType.setTypeName(edgeLabel.getLabel());
        edgeType.setTypeId(edgeLabel.getId());
        edgeType.setProperties(convertToDtoProperties(edgeLabel.getProperties()));
        for (EdgeRelation edgeRelation : edgeLabel.getRelations()) {
            BaseEdgeTypeVertexTypePairRelationsInner pair = new BaseEdgeTypeVertexTypePairRelationsInner();
            pair.setSourceVertex(edgeRelation.getSrcLabel());
            pair.setDestinationVertex(edgeRelation.getDstLabel());
            edgeType.addVertexTypePairRelationsItem(pair);
        }
        return edgeType;
    }

    private static Map<String, String> convertToPropertyMap(List<Property> properties) {
        Map<String, String> propertyMap = new HashMap<>();
        for (Property property : properties) {
            String value = extractValue(property.getValue());
            propertyMap.put(property.getName(), value);
        }
        return propertyMap;
    }

    private static List<GetPropertyMeta> convertToDtoProperties(
            List<com.alibaba.graphscope.groot.sdk.schema.Property> properties) {
        List<GetPropertyMeta> propertyMetas = new ArrayList<>();
        for (com.alibaba.graphscope.groot.sdk.schema.Property property : properties) {
            GetPropertyMeta propertyMeta = new GetPropertyMeta();
            propertyMeta.setPropertyName(property.getName());
            propertyMeta.setPropertyId(property.getId());
            propertyMeta.setPropertyType(convertToDtoDataType(property.getDataType()));
            propertyMetas.add(propertyMeta);
        }
        return propertyMetas;
    }

    private static GSDataType convertToDtoDataType(DataTypePb dataType) {
        switch (dataType) {
            case INT:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.SIGNED_INT32);
            case UINT:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.UNSIGNED_INT32);
            case LONG:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.SIGNED_INT64);
            case ULONG:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.UNSIGNED_INT64);
            case BOOL:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.BOOL);
            case FLOAT:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.FLOAT);
            case DOUBLE:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.DOUBLE);
            case STRING:
                // TODO: confirm the string type
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.STRING);
            case DATE32:
                // TODO: confirm the date type
                TemporalTypeTemporal date = new DateType("YYYY-MM-DD".toString());
                return new TemporalType(date);
            case TIMESTAMP_S:
                // TODO: confirm the timestamp type
                TemporalTypeTemporal timestamp = new TimeStampType("YYYY-MM-DD HH:MM:SS".toString());
                return new TemporalType(timestamp);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private static String extractValue(JsonNullable<Object> jsonNullable) {
        return jsonNullable.isPresent() ? jsonNullable.get().toString() : null;
    }

}
