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
package com.alibaba.maxgraph.compiler.api.schema;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.maxgraph.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.compiler.api.exception.GraphPropertyNotFoundException;

import java.util.List;
import java.util.Map;

public interface GraphSchema {
    GraphElement getElement(String label) throws GraphElementNotFoundException;

    GraphElement getElement(int labelId) throws GraphElementNotFoundException;

    List<GraphVertex> getVertexList();

    List<GraphEdge> getEdgeList();

    Integer getPropertyId(String propName) throws GraphPropertyNotFoundException;

    String getPropertyName(int propId) throws GraphPropertyNotFoundException;

    Map<GraphElement, GraphProperty> getPropertyList(String propName);

    Map<GraphElement, GraphProperty> getPropertyList(int propId);

    /**
     * Get the version of the schema
     *
     * @return The schema version
     */
    int getVersion();

    default String formatJson() {
        JSONObject jsonObject = new JSONObject();
        JSONArray typeArray = new JSONArray();
        for (GraphVertex vertex : this.getVertexList()) {
            JSONObject typeObject = new JSONObject();
            typeObject.put("id", vertex.getLabelId());
            typeObject.put("label", vertex.getLabel());
            typeObject.put("type", "VERTEX");
            JSONArray propArray = new JSONArray();
            for (GraphProperty property : vertex.getPropertyList()) {
                JSONObject propObject = new JSONObject();
                propObject.put("name", property.getName());
                propObject.put("id", property.getId());
                propObject.put("data_type", property.getDataType().toString());
                propArray.add(propObject);
            }
            typeObject.put("propertyDefList", propArray);

            JSONArray indexArray = new JSONArray();
            JSONObject indexObject = new JSONObject();
            JSONArray propertyNamesArray = new JSONArray();
            for (GraphProperty primaryKeyProp : vertex.getPrimaryKeyList()) {
                propertyNamesArray.add(primaryKeyProp.getName());
            }
            indexObject.put("propertyNames", propertyNamesArray);
            indexArray.add(indexObject);
            typeObject.put("indexes", indexArray);

            typeArray.add(typeObject);
        }

        for (GraphEdge edge : this.getEdgeList()) {
            JSONObject typeObject = new JSONObject();
            typeObject.put("id", edge.getLabelId());
            typeObject.put("label", edge.getLabel());
            typeObject.put("type", "EDGE");
            JSONArray propArray = new JSONArray();
            for (GraphProperty property : edge.getPropertyList()) {
                JSONObject propObject = new JSONObject();
                propObject.put("name", property.getName());
                propObject.put("id", property.getId());
                propObject.put("data_type", property.getDataType().toString());
                propArray.add(propObject);
            }
            typeObject.put("propertyDefList", propArray);

            JSONArray relationArray = new JSONArray();
            for (EdgeRelation relation : edge.getRelationList()) {
                JSONObject relationObject = new JSONObject();
                relationObject.put("srcVertexLabel", relation.getSource().getLabel());
                relationObject.put("dstVertexLabel", relation.getTarget().getLabel());
                relationArray.add(relationObject);
            }
            typeObject.put("rawRelationShips", relationArray);

            typeArray.add(typeObject);
        }

        jsonObject.put("types", typeArray);

        return jsonObject.toJSONString();
    }
}
