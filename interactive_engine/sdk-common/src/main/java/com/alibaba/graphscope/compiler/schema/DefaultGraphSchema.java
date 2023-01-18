/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.compiler.schema;

import com.alibaba.graphscope.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.graphscope.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.graphscope.compiler.api.schema.DataType;
import com.alibaba.graphscope.compiler.api.schema.EdgeRelation;
import com.alibaba.graphscope.compiler.api.schema.GraphEdge;
import com.alibaba.graphscope.compiler.api.schema.GraphElement;
import com.alibaba.graphscope.compiler.api.schema.GraphProperty;
import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.compiler.api.schema.GraphVertex;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DefaultGraphSchema implements GraphSchema {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGraphSchema.class);
    private Map<String, GraphVertex> vertexList;
    private Map<String, GraphEdge> edgeList;
    private Map<String, Integer> propNameToIdList;
    private Map<Integer, String> idToLabelList = Maps.newHashMap();

    public DefaultGraphSchema(
            Map<String, GraphVertex> vertexList,
            Map<String, GraphEdge> edgeList,
            Map<String, Integer> propNameToIdList) {
        this.vertexList = vertexList;
        this.edgeList = edgeList;
        vertexList.forEach((key, value) -> idToLabelList.put(value.getLabelId(), key));
        edgeList.forEach((key, value) -> idToLabelList.put(value.getLabelId(), key));
        this.propNameToIdList = propNameToIdList;
    }

    @Override
    public GraphElement getElement(String label) throws GraphElementNotFoundException {
        if (vertexList.containsKey(label)) {
            return vertexList.get(label);
        } else if (edgeList.containsKey(label)) {
            return edgeList.get(label);
        }

        throw new GraphElementNotFoundException("label " + label + " not exist");
    }

    @Override
    public GraphElement getElement(int labelId) throws GraphElementNotFoundException {
        if (idToLabelList.containsKey(labelId)) {
            return getElement(idToLabelList.get(labelId));
        }

        throw new GraphElementNotFoundException("label not exist for labelid " + labelId);
    }

    @Override
    public List<GraphVertex> getVertexList() {
        return Lists.newArrayList(vertexList.values());
    }

    @Override
    public List<GraphEdge> getEdgeList() {
        return Lists.newArrayList(edgeList.values());
    }

    @Override
    public Integer getPropertyId(String propName) throws GraphPropertyNotFoundException {
        if (propNameToIdList.containsKey(propName)) {
            return propNameToIdList.get(propName);
        }

        throw new GraphPropertyNotFoundException("property " + propName + " not exist");
    }

    @Override
    public String getPropertyName(int propId) throws GraphPropertyNotFoundException {
        for (Map.Entry<String, Integer> entry : propNameToIdList.entrySet()) {
            if (entry.getValue() == propId) {
                return entry.getKey();
            }
        }
        throw new GraphPropertyNotFoundException("property not exist for property id " + propId);
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(String propName) {
        Map<GraphElement, GraphProperty> elementPropertyList = Maps.newHashMap();
        vertexList.forEach(
                (key, value) -> {
                    for (GraphProperty property : value.getPropertyList()) {
                        if (property.getName().equals(propName)) {
                            elementPropertyList.put(value, property);
                        }
                    }
                });
        edgeList.forEach(
                (key, value) -> {
                    for (GraphProperty property : value.getPropertyList()) {
                        if (property.getName().equals(propName)) {
                            elementPropertyList.put(value, property);
                        }
                    }
                });
        return elementPropertyList;
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(int propId) {
        try {
            String propName = getPropertyName(propId);
            return getPropertyList(propName);
        } catch (Exception ignored) {
            return Maps.newHashMap();
        }
    }

    @Override
    public int getVersion() {
        return 0;
    }

    public static GraphSchema buildSchemaFromJson(String schemaJson) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode jsonNode = mapper.readTree(schemaJson);
            Map<String, GraphVertex> vertexList = Maps.newHashMap();
            Map<String, GraphEdge> edgeList = Maps.newHashMap();
            Map<String, Integer> propNameToIdList = Maps.newHashMap();
            JsonNode typeList = jsonNode.get("types");
            if (null != typeList) {
                int propId = 1;
                for (JsonNode typeObject : typeList) {
                    int labelId = typeObject.get("id").asInt();
                    String label = typeObject.get("label").asText();
                    String type = typeObject.get("type").asText();

                    Map<String, GraphProperty> namePropertyList = Maps.newHashMap();
                    List<GraphProperty> propertyList = Lists.newArrayList();
                    JsonNode propArray = typeObject.get("propertyDefList");
                    if (null != propArray) {
                        for (JsonNode propObject : propArray) {
                            String propName = propObject.get("name").asText();
                            int currPropId;
                            if (propObject.has("id")) {
                                currPropId = propObject.get("id").asInt();
                            } else {
                                currPropId = propId++;
                            }
                            String propDataTypeString = propObject.get("data_type").asText();
                            com.alibaba.graphscope.sdkcommon.meta.DataType dataType;
                            dataType =
                                    com.alibaba.graphscope.sdkcommon.meta.DataType.valueOf(
                                            propDataTypeString);
                            GraphProperty property =
                                    new DefaultGraphProperty(
                                            currPropId,
                                            propName,
                                            DataType.parseFromDataType(dataType));
                            propertyList.add(property);
                            namePropertyList.put(propName, property);
                            propNameToIdList.put(propName, currPropId);
                        }
                    } else {
                        logger.warn("There's no property def list in " + label);
                    }

                    if (type.equalsIgnoreCase("VERTEX")) {
                        List<GraphProperty> primaryPropertyList = Lists.newArrayList();

                        JsonNode indexArray = typeObject.get("indexes");
                        if (indexArray != null) {
                            for (JsonNode indexObject : indexArray) {
                                JsonNode priNameList = indexObject.get("propertyNames");
                                for (JsonNode pri : priNameList) {
                                    primaryPropertyList.add(namePropertyList.get(pri.asText()));
                                }
                            }
                        }
                        DefaultGraphVertex graphVertex =
                                new DefaultGraphVertex(
                                        labelId, label, propertyList, primaryPropertyList);
                        vertexList.put(label, graphVertex);
                    } else {
                        List<EdgeRelation> relationList = Lists.newArrayList();
                        JsonNode relationArray = typeObject.get("rawRelationShips");
                        if (null != relationArray) {
                            for (JsonNode relationObject : relationArray) {
                                String sourceLabel = relationObject.get("srcVertexLabel").asText();
                                String targetLabel = relationObject.get("dstVertexLabel").asText();
                                relationList.add(
                                        new DefaultEdgeRelation(
                                                vertexList.get(sourceLabel),
                                                vertexList.get(targetLabel)));
                            }
                        } else {
                            logger.warn("There's no relation def in edge " + label);
                        }
                        DefaultGraphEdge graphEdge =
                                new DefaultGraphEdge(labelId, label, propertyList, relationList);
                        edgeList.put(label, graphEdge);
                    }
                }
            } else {
                logger.error("Cant get types field in json[" + schemaJson + "]");
            }

            return new DefaultGraphSchema(vertexList, edgeList, propNameToIdList);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws JsonProcessingException {
        String schemaJson =
                "{\"partitionNum\": 2, \"types\": [{\"id\": 0, \"indexes\": [{\"propertyNames\":"
                    + " [\"id\"]}], \"label\": \"host\", \"propertyDefList\": [{\"data_type\":"
                    + " \"LONG\", \"id\": 6, \"name\": \"weight\"}, {\"data_type\": \"LONG\","
                    + " \"id\": 4, \"name\": \"id\"}], \"rawRelationShips\": [], \"type\":"
                    + " \"VERTEX\", \"valid_properties\": [1, 1]}, {\"id\": 1, \"indexes\": [],"
                    + " \"label\": \"connect\", \"propertyDefList\": [{\"data_type\": \"LONG\","
                    + " \"id\": 3, \"name\": \"eid\"}, {\"data_type\": \"LONG\", \"id\": 5,"
                    + " \"name\": \"src_label_id\"}, {\"data_type\": \"LONG\", \"id\": 2, \"name\":"
                    + " \"dst_label_id\"}, {\"data_type\": \"LONG\", \"id\": 1, \"name\":"
                    + " \"dist\"}], \"rawRelationShips\": [{\"dstVertexLabel\": \"host\","
                    + " \"srcVertexLabel\": \"host\"}], \"type\": \"EDGE\", \"valid_properties\":"
                    + " [1, 1, 1, 1]}], \"valid_edges\": [1], \"valid_vertices\": [1]}";
        GraphSchema schema = DefaultGraphSchema.buildSchemaFromJson(schemaJson);
        System.out.println(schema.formatJson());
    }
}
