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
package com.alibaba.maxgraph.compiler.schema;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.maxgraph.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.sdkcommon.exception.MaxGraphException;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
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

    public DefaultGraphSchema(Map<String, GraphVertex> vertexList, Map<String, GraphEdge> edgeList, Map<String, Integer> propNameToIdList) {
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
        vertexList.forEach((key, value) -> {
            for (GraphProperty property : value.getPropertyList()) {
                if (StringUtils.equals(property.getName(), propName)) {
                    elementPropertyList.put(value, property);
                }
            }
        });
        edgeList.forEach((key, value) -> {
            for (GraphProperty property : value.getPropertyList()) {
                if (StringUtils.equals(property.getName(), propName)) {
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
        JSONObject jsonObject = JSONObject.parseObject(schemaJson);
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        Map<String, GraphEdge> edgeList = Maps.newHashMap();
        Map<String, Integer> propNameToIdList = Maps.newHashMap();
        JSONArray typeList = jsonObject.getJSONArray("types");
        if (null != typeList) {
            int propId = 1;
            for (int i = 0; i < typeList.size(); i++) {
                JSONObject typeObject = typeList.getJSONObject(i);
                int labelId = typeObject.getInteger("id");
                String label = typeObject.getString("label");
                String type = typeObject.getString("type");

                Map<String, GraphProperty> namePropertyList = Maps.newHashMap();
                List<GraphProperty> propertyList = Lists.newArrayList();
                JSONArray propArray = typeObject.getJSONArray("propertyDefList");
                if (null != propArray) {
                    for (int j = 0; j < propArray.size(); j++) {
                        JSONObject propObject = propArray.getJSONObject(j);
                        String propName = propObject.getString("name");
                        Integer currPropId = propObject.getInteger("id");
                        if (null == currPropId) {
                            currPropId = propId++;
                        }
                        String propDataTypeString = propObject.getString("data_type");
                        com.alibaba.maxgraph.sdkcommon.meta.DataType dataType;
                        if (StringUtils.startsWith(propDataTypeString, "LIST")) {
                            dataType = new com.alibaba.maxgraph.sdkcommon.meta.DataType(InternalDataType.LIST);
                            try {
                                dataType.setExpression(StringUtils.removeEnd(StringUtils.removeStart(propDataTypeString, "LIST<"), ">"));
                            } catch (MaxGraphException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            dataType = com.alibaba.maxgraph.sdkcommon.meta.DataType.valueOf(propDataTypeString);
                        }
                        GraphProperty property = new DefaultGraphProperty(currPropId,
                                propName,
                                DataType.parseFromDataType(dataType));
                        propertyList.add(property);
                        namePropertyList.put(propName, property);
                        propNameToIdList.put(propName, currPropId);
                    }
                } else {
                    logger.warn("There's no property def list in " + label);
                }

                if (StringUtils.equals(type, "VERTEX")) {
                    List<GraphProperty> primaryPropertyList = Lists.newArrayList();
                    JSONArray indexArray = typeObject.getJSONArray("indexes");
                    if (indexArray != null) {
                        for (int k = 0; k < indexArray.size(); k++) {
                            JSONObject indexObject = indexArray.getJSONObject(k);
                            JSONArray priNameList = indexObject.getJSONArray("propertyNames");
                            for (int j = 0; j < priNameList.size(); j++) {
                                primaryPropertyList.add(namePropertyList.get(priNameList.getString(j)));
                            }
                        }
                    }
                    DefaultGraphVertex graphVertex = new DefaultGraphVertex(labelId,
                            label,
                            propertyList,
                            primaryPropertyList);
                    vertexList.put(label, graphVertex);
                } else {
                    List<EdgeRelation> relationList = Lists.newArrayList();
                    JSONArray relationArray = typeObject.getJSONArray("rawRelationShips");
                    if (null != relationArray) {
                        for (int k = 0; k < relationArray.size(); k++) {
                            JSONObject relationObject = relationArray.getJSONObject(k);
                            String sourceLabel = relationObject.getString("srcVertexLabel");
                            String targetLabel = relationObject.getString("dstVertexLabel");
                            relationList.add(new DefaultEdgeRelation(vertexList.get(sourceLabel), vertexList.get(targetLabel)));
                        }
                    } else {
                        logger.warn("There's no relation def in edge " + label);
                    }
                    DefaultGraphEdge graphEdge = new DefaultGraphEdge(labelId,
                            label,
                            propertyList,
                            relationList);
                    edgeList.put(label, graphEdge);
                }
            }
        } else {
            logger.error("Cant get types field in json[" + schemaJson + "]");
        }

        return new DefaultGraphSchema(vertexList, edgeList, propNameToIdList);
    }
}
