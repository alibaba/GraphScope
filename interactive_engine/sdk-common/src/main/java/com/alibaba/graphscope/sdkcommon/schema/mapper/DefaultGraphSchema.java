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
package com.alibaba.graphscope.sdkcommon.schema.mapper;

import com.alibaba.graphscope.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.graphscope.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.graphscope.compiler.api.schema.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Default graph schema in memory for testing */
public class DefaultGraphSchema implements GraphSchema {
    private static final DefaultGraphSchema SCHEMA = new DefaultGraphSchema();

    private Map<String, GraphVertex> vertexList;
    private Map<String, GraphEdge> edgeList;

    public DefaultGraphSchema() {
        this.vertexList = new HashMap<>();
        this.edgeList = new HashMap<>();
    }

    public static DefaultGraphSchema getSchema() {
        return SCHEMA;
    }

    public void createVertexType(GraphVertex graphVertex) {
        this.vertexList.put(graphVertex.getLabel(), graphVertex);
    }

    public void createEdgeType(GraphEdge graphEdge) {
        this.edgeList.put(graphEdge.getLabel(), graphEdge);
    }

    public void dropVertexType(String label) {
        this.vertexList.remove(label);
    }

    public void dropEdgeType(String label) {
        this.edgeList.remove(label);
    }

    public void addProperty(String label, GraphProperty property) {
        List<GraphProperty> propertyList = this.getElement(label).getPropertyList();
        propertyList.add(property);
    }

    public void dropProperty(String label, String property) {
        this.getElement(label)
                .getPropertyList()
                .removeIf(v -> v.getName().equals(property));
    }

    @Override
    public GraphElement getElement(String label) throws GraphElementNotFoundException {
        if (vertexList.containsKey(label)) {
            return vertexList.get(label);
        }
        if (edgeList.containsKey(label)) {
            return edgeList.get(label);
        }
        throw new GraphElementNotFoundException("cant found element for label " + label);
    }

    @Override
    public GraphElement getElement(int labelId) throws GraphElementNotFoundException {
        for (GraphVertex graphVertex : this.vertexList.values()) {
            if (graphVertex.getLabelId() == labelId) {
                return graphVertex;
            }
        }
        for (GraphEdge graphEdge : this.edgeList.values()) {
            if (graphEdge.getLabelId() == labelId) {
                return graphEdge;
            }
        }
        throw new GraphElementNotFoundException("Not found schema element for label id " + labelId);
    }

    @Override
    public List<GraphVertex> getVertexList() {
        return new ArrayList<>(vertexList.values());
    }

    @Override
    public List<GraphEdge> getEdgeList() {
        return new ArrayList<>(this.edgeList.values());
    }

    @Override
    public Integer getPropertyId(String propertyName) throws GraphPropertyNotFoundException {
        Map<Integer, Integer> typePropertyIds = new HashMap<>();
        for (GraphVertex graphVertex : this.vertexList.values()) {
            try {
                GraphProperty property = graphVertex.getProperty(propertyName);
                typePropertyIds.put(graphVertex.getLabelId(), property.getId());
            } catch (Exception ignored) {
            }
        }
        for (GraphEdge graphEdge : this.edgeList.values()) {
            try {
                GraphProperty property = graphEdge.getProperty(propertyName);
                typePropertyIds.put(graphEdge.getLabelId(), property.getId());
            } catch (Exception ignored) {
            }
        }
        return typePropertyIds.values().iterator().next();
    }

    @Override
    public String getPropertyName(int propertyId) throws GraphPropertyNotFoundException {
        Map<String, String> labelPropertyNameList = new HashMap<>();
        for (GraphElement element : this.vertexList.values()) {
            try {
                labelPropertyNameList.put(
                        element.getLabel(), element.getProperty(propertyId).getName());
            } catch (Exception ignored) {
            }
        }
        for (GraphElement element : this.edgeList.values()) {
            try {
                labelPropertyNameList.put(
                        element.getLabel(), element.getProperty(propertyId).getName());
            } catch (Exception ignored) {
            }
        }
        return labelPropertyNameList.values().iterator().next();
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(String propName) {
        Map<GraphElement, GraphProperty> elementPropertyList = new HashMap<>();
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
            return new HashMap<>();
        }
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public String toString() {
        return "DefaultGraphSchema{" +
                "vertexList=" + vertexList +
                ", edgeList=" + edgeList +
                '}';
    }
}
