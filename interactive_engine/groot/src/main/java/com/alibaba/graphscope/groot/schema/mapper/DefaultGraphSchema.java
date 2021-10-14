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
package com.alibaba.graphscope.groot.schema.mapper;

import com.alibaba.maxgraph.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/** Default graph schema in memory for testing */
public class DefaultGraphSchema implements GraphSchema {
    private static final DefaultGraphSchema SCHEMA = new DefaultGraphSchema();

    private Map<String, GraphVertex> vertexList;
    private Map<String, GraphEdge> edgeList;

    public DefaultGraphSchema() {
        this.vertexList = Maps.newHashMap();
        this.edgeList = Maps.newHashMap();
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
        List<GraphProperty> propertyList =
                (List<GraphProperty>) this.getElement(label).getPropertyList();
        propertyList.add(property);
    }

    public void dropProperty(String label, String property) {
        this.getElement(label)
                .getPropertyList()
                .removeIf(v -> StringUtils.equals(v.getName(), property));
    }

    @Override
    public GraphElement getElement(String label) throws GraphElementNotFoundException {
        Map<String, GraphElement> elementList = Maps.newHashMap(this.vertexList);
        elementList.putAll(this.edgeList);
        if (elementList.containsKey(label)) {
            return elementList.get(label);
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
        return Lists.newArrayList(this.vertexList.values());
    }

    @Override
    public List<GraphEdge> getEdgeList() {
        return Lists.newArrayList(this.edgeList.values());
    }

    @Override
    public Integer getPropertyId(String propertyName) throws GraphPropertyNotFoundException {
        Map<Integer, Integer> typePropertyIds = Maps.newHashMap();
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
        Map<String, String> labelPropertyNameList = Maps.newHashMap();
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
        Map<GraphElement, GraphProperty> elementPropertyList = Maps.newHashMap();
        vertexList.forEach(
                (key, value) -> {
                    for (GraphProperty property : value.getPropertyList()) {
                        if (StringUtils.equals(property.getName(), propName)) {
                            elementPropertyList.put(value, property);
                        }
                    }
                });
        edgeList.forEach(
                (key, value) -> {
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
        return 1;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("vertexTypes", vertexList)
                .add("edgeTypes", edgeList)
                .toString();
    }
}
