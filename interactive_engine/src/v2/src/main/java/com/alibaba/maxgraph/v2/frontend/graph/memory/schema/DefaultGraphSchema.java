package com.alibaba.maxgraph.v2.frontend.graph.memory.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Default graph schema in memory for testing
 */
public class DefaultGraphSchema implements GraphSchema {
    private static final DefaultGraphSchema SCHEMA = new DefaultGraphSchema();

    private Map<String, VertexType> vertexTypes;
    private Map<String, EdgeType> edgeTypes;

    public DefaultGraphSchema() {
        this.vertexTypes = Maps.newHashMap();
        this.edgeTypes = Maps.newHashMap();
        this.vertexTypes = Maps.newHashMap();
        this.edgeTypes = Maps.newHashMap();
    }

    public static DefaultGraphSchema getSchema() {
        return SCHEMA;
    }

    public void createVertexType(VertexType vertexType) {
        this.vertexTypes.put(vertexType.getLabel(), vertexType);
    }

    public void createEdgeType(EdgeType edgeType) {
        this.edgeTypes.put(edgeType.getLabel(), edgeType);
    }

    public void dropVertexType(String label) {
        this.vertexTypes.remove(label);
    }

    public void dropEdgeType(String label) {
        this.edgeTypes.remove(label);
    }

    public void addProperty(String label, GraphProperty property) {
        List<GraphProperty> propertyList = (List<GraphProperty>) this.getSchemaElement(label).getPropertyList();
        propertyList.add(property);
    }

    public void dropProperty(String label, String property) {
        this.getSchemaElement(label)
                .getPropertyList().removeIf(v -> StringUtils.equals(v.getName(), property));
    }

    @Override
    public SchemaElement getSchemaElement(String label) throws GraphElementNotFoundException {
        Map<String, SchemaElement> elementList = Maps.newHashMap(this.vertexTypes);
        elementList.putAll(this.edgeTypes);
        if (elementList.containsKey(label)) {
            return elementList.get(label);
        }
        throw new GraphElementNotFoundException("cant found element for label " + label);
    }

    @Override
    public SchemaElement getSchemaElement(int labelId) throws GraphElementNotFoundException {
        for (VertexType vertexType : this.vertexTypes.values()) {
            if (vertexType.getLabelId() == labelId) {
                return vertexType;
            }
        }
        for (EdgeType edgeType : this.edgeTypes.values()) {
            if (edgeType.getLabelId() == labelId) {
                return edgeType;
            }
        }
        throw new GraphElementNotFoundException("Not found schema element for label id " + labelId);
    }

    @Override
    public List<VertexType> getVertexTypes() {
        return Lists.newArrayList(this.vertexTypes.values());
    }

    @Override
    public List<EdgeType> getEdgeTypes() {
        return Lists.newArrayList(this.edgeTypes.values());
    }

    @Override
    public Map<Integer, Integer> getPropertyId(String propertyName) throws GraphPropertyNotFoundException {
        Map<Integer, Integer> typePropertyIds = Maps.newHashMap();
        for (VertexType vertexType : this.vertexTypes.values()) {
            try {
                GraphProperty property = vertexType.getProperty(propertyName);
                typePropertyIds.put(vertexType.getLabelId(), property.getId());
            } catch (Exception ignored) {
            }
        }
        for (EdgeType edgeType : this.edgeTypes.values()) {
            try {
                GraphProperty property = edgeType.getProperty(propertyName);
                typePropertyIds.put(edgeType.getLabelId(), property.getId());
            } catch (Exception ignored) {
            }
        }
        return typePropertyIds;
    }

    @Override
    public Map<String, String> getPropertyName(int propertyId) throws GraphPropertyNotFoundException {
        Map<String, String> labelPropertyNameList = Maps.newHashMap();
        for (SchemaElement element : this.vertexTypes.values()) {
            try {
                labelPropertyNameList.put(element.getLabel(), element.getProperty(propertyId).getName());
            } catch (Exception ignored) {
            }
        }
        for (SchemaElement element : this.edgeTypes.values()) {
            try {
                labelPropertyNameList.put(element.getLabel(), element.getProperty(propertyId).getName());
            } catch (Exception ignored) {
            }

        }
        return labelPropertyNameList;
    }

    @Override
    public Map<Integer, GraphProperty> getPropertyDefinitions(String propertyName) {
        Map<Integer, GraphProperty> propertyList = Maps.newHashMap();
        for (SchemaElement element : this.vertexTypes.values()) {
            try {
                GraphProperty property = element.getProperty(propertyName);
                if (null != property) {
                    propertyList.put(element.getLabelId(), property);
                }
            } catch (Exception ignored) {
            }
        }
        for (SchemaElement element : this.edgeTypes.values()) {
            try {
                GraphProperty property = element.getProperty(propertyName);
                if (null != property) {
                    propertyList.put(element.getLabelId(), property);
                }
            } catch (Exception ignored) {
            }
        }
        return propertyList;
    }

    @Override
    public GraphProperty getPropertyDefinition(int labelId, int propertyId) {
        SchemaElement schemaElement = getSchemaElement(labelId);
        return schemaElement.getProperty(propertyId);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("vertexTypes", vertexTypes)
                .add("edgeTypes", edgeTypes)
                .toString();
    }
}
