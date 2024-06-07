/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.groot.common.schema.api.*;
import com.alibaba.graphscope.groot.common.schema.impl.*;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.alibaba.graphscope.groot.common.schema.wrapper.EdgeKind;
import com.alibaba.graphscope.groot.common.schema.wrapper.LabelId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.yaml.snakeyaml.Yaml;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class Utils {
    /**
     * build {@link GraphSchema} from schema yaml (in gs interactive format)
     * @param schemaYaml
     * @return
     */
    public static final GraphSchema buildSchemaFromYaml(String schemaYaml) {
        Yaml yaml = new Yaml();
        Map<String, Object> yamlAsMap = yaml.load(schemaYaml);
        Map<String, Object> schemaMap =
                (Map<String, Object>)
                        Objects.requireNonNull(
                                yamlAsMap.get("schema"), "schema not exist in yaml config");
        Map<String, GraphVertex> vertexMap = Maps.newHashMap();
        Map<String, GraphEdge> edgeMap = Maps.newHashMap();
        Map<String, Integer> propNameToIdMap = Maps.newHashMap();
        GSDataTypeConvertor<DataType> typeConvertor =
                GSDataTypeConvertor.Factory.create(DataType.class, null);
        builderGraphElementFromYaml(
                (List)
                        Objects.requireNonNull(
                                schemaMap.get("vertex_types"),
                                "vertex_types not exist in yaml config"),
                "VERTEX",
                vertexMap,
                edgeMap,
                propNameToIdMap,
                typeConvertor);
        if (schemaMap.get("edge_types") != null) {
            builderGraphElementFromYaml(
                    (List)
                            Objects.requireNonNull(
                                    schemaMap.get("edge_types"),
                                    "edge_types not exist in yaml config"),
                    "EDGE",
                    vertexMap,
                    edgeMap,
                    propNameToIdMap,
                    typeConvertor);
        }
        return new DefaultGraphSchema(vertexMap, edgeMap, propNameToIdMap);
    }

    public static final void builderGraphElementFromYaml(
            List elementList,
            String type,
            Map<String, GraphVertex> vertexMap,
            Map<String, GraphEdge> edgeMap,
            Map<String, Integer> propNameToIdMap,
            GSDataTypeConvertor<DataType> typeConvertor) {
        for (Object element : elementList) {
            if (element instanceof Map) {
                Map<String, Object> elementMap = (Map<String, Object>) element;
                String label = (String) elementMap.get("type_name");
                int labelId =
                        (int)
                                Objects.requireNonNull(
                                        elementMap.get("type_id"),
                                        "type_id not exist in yaml config");
                List<GraphProperty> propertyList = Lists.newArrayList();
                List propertyNodes = (List) elementMap.get("properties");
                if (propertyNodes != null) {
                    for (Object property : propertyNodes) {
                        if (property instanceof Map) {
                            Map<String, Object> propertyMap = (Map<String, Object>) property;
                            String propertyName =
                                    (String)
                                            Objects.requireNonNull(
                                                    propertyMap.get("property_name"),
                                                    "property_name not exist in yaml config");
                            int propertyId =
                                    (int)
                                            Objects.requireNonNull(
                                                    propertyMap.get("property_id"),
                                                    "property_id not exist in yaml config");
                            propNameToIdMap.put(propertyName, propertyId);
                            propertyList.add(
                                    new DefaultGraphProperty(
                                            propertyId,
                                            propertyName,
                                            toDataType(
                                                    propertyMap.get("property_type"),
                                                    typeConvertor)));
                        }
                    }
                }
                List primaryKeyNodes = (List) elementMap.get("primary_keys");
                List<String> primaryKeyList =
                        (primaryKeyNodes == null)
                                ? ImmutableList.of()
                                : (List<String>)
                                        primaryKeyNodes.stream()
                                                .map(k -> k.toString())
                                                .collect(Collectors.toList());
                if (type.equals("EDGE")) {
                    List<EdgeRelation> relations = Lists.newArrayList();
                    List relationNodes = (List) elementMap.get("vertex_type_pair_relations");
                    for (Object relation : relationNodes) {
                        if (relation instanceof Map) {
                            Map<String, Object> relationMap = (Map<String, Object>) relation;
                            String sourceLabel = relationMap.get("source_vertex").toString();
                            String dstLabel = relationMap.get("destination_vertex").toString();
                            relations.add(
                                    new DefaultEdgeRelation(
                                            vertexMap.get(sourceLabel), vertexMap.get(dstLabel)));
                        }
                    }
                    edgeMap.put(
                            label, new DefaultGraphEdge(labelId, label, propertyList, relations));
                } else if (type.equals("VERTEX")) {
                    vertexMap.put(
                            label,
                            new DefaultGraphVertex(labelId, label, propertyList, primaryKeyList));
                }
            }
        }
    }

    public static DataType toDataType(Object type, GSDataTypeConvertor<DataType> typeConvertor) {
        return typeConvertor.convert(new GSDataTypeDesc((Map<String, Object>) type));
    }

    /**
     * build {@link GraphSchema} from schema json (in ir core format)
     * @param schemaJson
     * @return
     */
    public static final GraphSchema buildSchemaFromJson(String schemaJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(schemaJson);
            Map<String, GraphVertex> vertexMap = Maps.newHashMap();
            Map<String, GraphEdge> edgeMap = Maps.newHashMap();
            Map<String, Integer> propNameToIdMap = Maps.newHashMap();
            buildGraphElementFromJson(
                    jsonNode.get("entities"), "VERTEX", vertexMap, edgeMap, propNameToIdMap);
            buildGraphElementFromJson(
                    jsonNode.get("relations"), "EDGE", vertexMap, edgeMap, propNameToIdMap);
            return new DefaultGraphSchema(vertexMap, edgeMap, propNameToIdMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final void buildGraphElementFromJson(
            JsonNode elementList,
            String type,
            Map<String, GraphVertex> vertexMap,
            Map<String, GraphEdge> edgeMap,
            Map<String, Integer> propNameToIdMap) {
        Objects.requireNonNull(elementList);
        Iterator var1 = elementList.iterator();

        while (var1.hasNext()) {
            JsonNode typeObject = (JsonNode) var1.next();
            int labelId = typeObject.get("label").get("id").asInt();
            String label = typeObject.get("label").get("name").asText();
            List<GraphProperty> propertyList = Lists.newArrayList();
            List<String> primaryKeyList = Lists.newArrayList();
            JsonNode columnArray = typeObject.get("columns");
            Objects.requireNonNull(columnArray, "There's no property def list in " + label);
            Iterator var2 = columnArray.iterator();

            while (var2.hasNext()) {
                JsonNode column = (JsonNode) var2.next();
                String propName = column.get("key").get("name").asText();
                int propId = column.get("key").get("id").asInt();
                propNameToIdMap.put(propName, propId);
                int propTypeId = column.get("data_type").asInt();
                GraphProperty property =
                        new DefaultGraphProperty(propId, propName, toDataType(propTypeId));
                propertyList.add(property);
                boolean isPrimaryKey = column.get("is_primary_key").asBoolean();
                if (isPrimaryKey) {
                    primaryKeyList.add(propName);
                }
            }
            if (type.equals("EDGE")) {
                JsonNode entityPairs = typeObject.get("entity_pairs");
                Iterator var3 = entityPairs.iterator();
                List<EdgeRelation> relations = Lists.newArrayList();
                while (var3.hasNext()) {
                    JsonNode pair = (JsonNode) var3.next();
                    String sourceLabel = pair.get("src").get("name").asText();
                    String dstLabel = pair.get("dst").get("name").asText();
                    relations.add(
                            new DefaultEdgeRelation(
                                    vertexMap.get(sourceLabel), vertexMap.get(dstLabel)));
                }
                edgeMap.put(label, new DefaultGraphEdge(labelId, label, propertyList, relations));
            } else {
                vertexMap.put(
                        label,
                        new DefaultGraphVertex(labelId, label, propertyList, primaryKeyList));
            }
        }
    }

    public static final DataType toDataType(int ordinal) {
        switch (ordinal) {
            case 0:
                return DataType.BOOL;
            case 1:
                return DataType.INT;
            case 2:
                return DataType.LONG;
            case 3:
                return DataType.DOUBLE;
            case 4:
                return DataType.STRING;
            case 5:
                return DataType.BYTES;
            case 6:
                return DataType.INT_LIST;
            case 7:
                return DataType.LONG_LIST;
            case 8:
                return DataType.DOUBLE_LIST;
            case 9:
                return DataType.STRING_LIST;
            case 12:
                return DataType.DATE;
            case 13:
                return DataType.TIME32;
            case 14:
                return DataType.TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        "convert from ir core type " + ordinal + " to DataType is unsupported yet");
        }
    }

    /**
     * build {@link GraphStatistics} from statistics json
     * @param statisticsJson
     * @return
     */
    public static final GraphStatistics buildStatisticsFromJson(String statisticsJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(statisticsJson);
            Map<LabelId, Long> vertexTypeCounts = Maps.newHashMap();
            Map<EdgeKind, Long> edgeTypeCounts = Maps.newHashMap();
            Map<String, Integer> vertexTypeNameIdMap = Maps.newHashMap();
            Long num_vertices = jsonNode.get("total_vertex_count").asLong();
            Long num_edges = jsonNode.get("total_edge_count").asLong();
            JsonNode vertexTypeCountsNode = jsonNode.get("vertex_type_statistics");
            JsonNode edgeTypeCountsNode = jsonNode.get("edge_type_statistics");
            buildGraphElementStatisticsFromJson(
                    vertexTypeCountsNode,
                    "VERTEX",
                    vertexTypeCounts,
                    edgeTypeCounts,
                    vertexTypeNameIdMap);
            buildGraphElementStatisticsFromJson(
                    edgeTypeCountsNode,
                    "EDGE",
                    vertexTypeCounts,
                    edgeTypeCounts,
                    vertexTypeNameIdMap);

            return new DefaultGraphStatistics(
                    vertexTypeCounts, edgeTypeCounts, num_vertices, num_edges);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final void buildGraphElementStatisticsFromJson(
            JsonNode typeCountsNode,
            String type,
            Map<LabelId, Long> vertexTypeCounts,
            Map<EdgeKind, Long> edgeTypeCounts,
            Map<String, Integer> vertexTypeNameIdMap) {
        Iterator var1 = typeCountsNode.iterator();
        while (var1.hasNext()) {
            JsonNode typeStatisticJsonNode = (JsonNode) var1.next();
            int typeId = typeStatisticJsonNode.get("type_id").asInt();
            String typeName = typeStatisticJsonNode.get("type_name").asText();
            if (type.equals("VERTEX")) {
                Long typeCount = typeStatisticJsonNode.get("count").asLong();
                vertexTypeCounts.put(new LabelId(typeId), typeCount);
                vertexTypeNameIdMap.put(typeName, typeId);
            } else {
                JsonNode entityPairs = typeStatisticJsonNode.get("vertex_type_pair_statistics");
                Iterator var2 = entityPairs.iterator();
                while (var2.hasNext()) {
                    JsonNode pair = (JsonNode) var2.next();
                    String sourceLabel = pair.get("source_vertex").asText();
                    String dstLabel = pair.get("destination_vertex").asText();
                    Long typeCount = pair.get("count").asLong();
                    EdgeKind edgeKind =
                            EdgeKind.newBuilder()
                                    .setEdgeLabelId(new LabelId(typeId))
                                    .setSrcVertexLabelId(
                                            new LabelId(vertexTypeNameIdMap.get(sourceLabel)))
                                    .setDstVertexLabelId(
                                            new LabelId(vertexTypeNameIdMap.get(dstLabel)))
                                    .build();
                    edgeTypeCounts.put(edgeKind, typeCount);
                }
            }
        }
    }
}
