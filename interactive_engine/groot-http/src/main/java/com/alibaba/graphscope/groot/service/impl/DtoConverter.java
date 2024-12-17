package com.alibaba.graphscope.groot.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openapitools.jackson.nullable.JsonNullable;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.Property;
import com.alibaba.graphscope.groot.service.models.VertexRequest;

public class DtoConverter {
    public static Vertex convertToVertex(VertexRequest vertexRequest) {
        String label = vertexRequest.getLabel();
        Map<String, String> propertyMap = convertToPropertyMap(vertexRequest.getProperties());
        Map<String, String> primaryKeyValues = convertToPropertyMap(vertexRequest.getPrimaryKeyValues());
        propertyMap.putAll(primaryKeyValues);
        return new Vertex(label, propertyMap);
    }

    public static Vertex convertToVertex(String label, List<Property> pkValues) {
        Map<String, String> propertyMap = convertToPropertyMap(pkValues);
        return new Vertex(label, propertyMap);
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

    public static Edge convertToEdge(String edgeLabel, String srcLabel, String dstLabel, List<Property> srcPkValues,
            List<Property> dstPkValues) {
        Map<String, String> srcPkMap = convertToPropertyMap(srcPkValues);
        Map<String, String> dstPkMap = convertToPropertyMap(dstPkValues);
        return new Edge(edgeLabel, srcLabel, dstLabel, srcPkMap, dstPkMap);
    }

    private static Map<String, String> convertToPropertyMap(List<Property> properties) {
        Map<String, String> propertyMap = new HashMap<>();
        for (Property property : properties) {
            String value = extractValue(property.getValue());
            propertyMap.put(property.getName(), value);
        }
        return propertyMap;
    }

    private static String extractValue(JsonNullable<Object> jsonNullable) {
        return jsonNullable.isPresent() ? jsonNullable.get().toString() : null;
    }

}
