package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.schema.api.GraphElement;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Type implements GraphElement {
    public int typeId;
    public String typeName;
    public String description;
    public List<String> primaryKeys;
    public List<Property> properties;

    public int versionId;

    @Override
    public String toString() {
        return "Type{"
                + "typeId="
                + typeId
                + ", typeName='"
                + typeName
                + '\''
                + ", description='"
                + description
                + '\''
                + ", primaryKeys="
                + primaryKeys
                + ", properties="
                + properties
                + '}';
    }

    @Override
    @JsonIgnore
    public String getLabel() {
        return typeName;
    }

    @Override
    @JsonIgnore
    public int getLabelId() {
        return typeId;
    }

    @Override
    @JsonIgnore
    public List<GraphProperty> getPropertyList() {
        return new ArrayList<>(properties);
    }

    @Override
    public GraphProperty getProperty(int propId) {
        for (Property property : properties) {
            if (property.getId() == propId) {
                return property;
            }
        }
        return null;
    }

    @Override
    public GraphProperty getProperty(String propName) {
        for (Property property : properties) {
            if (Objects.equals(property.getName(), propName)) {
                return property;
            }
        }
        return null;
    }

    @Override
    @JsonIgnore
    public int getVersionId() {
        return versionId;
    }

    @Override
    @JsonIgnore
    public List<GraphProperty> getPrimaryKeyList() {
        if (primaryKeys == null) {
            return new ArrayList<>();
        }
        return properties.stream()
                .filter(property -> primaryKeys.contains(property.getName()))
                .collect(Collectors.toList());
    }

    @Override
    @JsonIgnore
    public List<String> getPrimaryKeyNameList() {
        return primaryKeys;
    }
}
