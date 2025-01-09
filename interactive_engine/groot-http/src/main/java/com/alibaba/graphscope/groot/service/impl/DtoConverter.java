package com.alibaba.graphscope.groot.service.impl;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.EdgeLabel;
import com.alibaba.graphscope.groot.sdk.schema.EdgeLabel.EdgeRelation;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.sdk.schema.VertexLabel;
import com.alibaba.graphscope.groot.service.models.BaseEdgeTypeVertexTypePairRelationsInner;
import com.alibaba.graphscope.groot.service.models.DateType;
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.DeleteVertexRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.GSDataType;
import com.alibaba.graphscope.groot.service.models.GetEdgeType;
import com.alibaba.graphscope.groot.service.models.GetPropertyMeta;
import com.alibaba.graphscope.groot.service.models.GetVertexType;
import com.alibaba.graphscope.groot.service.models.PrimitiveType;
import com.alibaba.graphscope.groot.service.models.Property;
import com.alibaba.graphscope.groot.service.models.StringType;
import com.alibaba.graphscope.groot.service.models.TemporalType;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.alibaba.graphscope.groot.service.models.TimeStampType;
import com.alibaba.graphscope.groot.service.models.VertexRequest;
import com.alibaba.graphscope.proto.groot.DataTypePb;

import org.openapitools.jackson.nullable.JsonNullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DtoConverter {
    public static Vertex convertToVertex(VertexRequest vertexRequest) {
        String label = vertexRequest.getLabel();
        Map<String, String> propertyMap = convertToPropertyMap(vertexRequest.getProperties());
        Map<String, String> primaryKeyValues =
                convertToPropertyMap(vertexRequest.getPrimaryKeyValues());
        propertyMap.putAll(primaryKeyValues);
        return new Vertex(label, propertyMap);
    }

    public static Vertex convertToVertex(DeleteVertexRequest deleteVertexRequest) {
        String label = deleteVertexRequest.getLabel();
        Map<String, String> primaryKeyValues =
                convertToPropertyMap(deleteVertexRequest.getPrimaryKeyValues());
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
        Map<String, String> srcPkMap =
                convertToPropertyMap(deleteEdgeRequest.getSrcPrimaryKeyValues());
        Map<String, String> dstPkMap =
                convertToPropertyMap(deleteEdgeRequest.getDstPrimaryKeyValues());
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
                    throw new IllegalArgumentException(
                            "Unsupported primitive type: " + primitiveType);
            }
        } else if (dataType instanceof StringType) {
            return DataTypePb.STRING;
        } else if (dataType instanceof TemporalType) {
            TemporalType temporalType = (TemporalType) dataType;
            TemporalTypeTemporal temporal = temporalType.getTemporal();
            if (temporal instanceof DateType) {
                return DataTypePb.DATE32;
            } else if (temporal instanceof TimeStampType) {
                return DataTypePb.TIMESTAMP_MS;
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
        vertexType.setPrimaryKeys(
                vertexLabel.getProperties().stream()
                        .filter(p -> p.isPrimaryKey())
                        .map(p -> p.getName())
                        .collect(Collectors.toList()));
        return vertexType;
    }

    public static GetEdgeType convertToDtoEdgeType(EdgeLabel edgeLabel) {
        GetEdgeType edgeType = new GetEdgeType();
        edgeType.setTypeName(edgeLabel.getLabel());
        edgeType.setTypeId(edgeLabel.getId());
        edgeType.setProperties(convertToDtoProperties(edgeLabel.getProperties()));
        for (EdgeRelation edgeRelation : edgeLabel.getRelations()) {
            BaseEdgeTypeVertexTypePairRelationsInner pair =
                    new BaseEdgeTypeVertexTypePairRelationsInner();
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
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.SIGNED_INT32, "PrimitiveType");
            case UINT:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.UNSIGNED_INT32, "PrimitiveType");
            case LONG:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.SIGNED_INT64, "PrimitiveType");
            case ULONG:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.UNSIGNED_INT64, "PrimitiveType");
            case BOOL:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.BOOL, "PrimitiveType");
            case FLOAT:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.FLOAT, "PrimitiveType");
            case DOUBLE:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.DOUBLE, "PrimitiveType");
            case STRING:
                return new PrimitiveType(PrimitiveType.PrimitiveTypeEnum.STRING, "PrimitiveType");
            case DATE32:
                TemporalTypeTemporal date = new DateType("YYYY-MM-DD".toString(), "DateType");
                return new TemporalType(date, "TemporalType");
            case TIMESTAMP_MS:
                // TODO: confirm the format of timestamp? should be int64 milliseconds since
                // 1970-01-01 00:00:00.000000?
                TemporalTypeTemporal timestamp =
                        new TimeStampType("YYYY-MM-DD HH:MM:SS".toString(), "TimeStampType");
                return new TemporalType(timestamp, "TemporalType");
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private static String extractValue(JsonNullable<Object> jsonNullable) {
        return jsonNullable.isPresent() ? jsonNullable.get().toString() : null;
    }
}
