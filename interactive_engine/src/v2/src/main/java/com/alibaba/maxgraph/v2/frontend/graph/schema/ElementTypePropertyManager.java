package com.alibaba.maxgraph.v2.frontend.graph.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Element type label name and property definitions manager
 */
public class ElementTypePropertyManager {
    private String label;
    private List<GraphProperty> propertyDefinitions;
    private String comment;

    public ElementTypePropertyManager(String label) {
        this.label = label;
        this.propertyDefinitions = Lists.newArrayList();
    }

    /**
     * Check the given proeprty name exists in the property list
     *
     * @param name The given property name
     * @return The exist flag result
     */
    public boolean checkPropertyExist(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new GraphCreateSchemaException("property name cant be empty");
        }

        for (GraphProperty property : propertyDefinitions) {
            if (StringUtils.equals(name, property.getName())) {
                return true;
            }
        }

        return false;
    }

    private DataType getDataType(String dataType) {
        try {
            return DataType.valueOf(dataType);
        }catch (Exception e) {
            throw new GraphCreateSchemaException("parse data type " + dataType + " failed", e);
        }
    }

    public void addProperty(String propertyName, String dataType, String comment) {
        if (checkPropertyExist(propertyName)) {
            throw new GraphCreateSchemaException("there's duplicate property " + propertyName + " in type " + label);
        }
        GraphProperty property = new DefaultGraphProperty(propertyName,
                0,
                this.getDataType(StringUtils.upperCase(dataType)),
                comment,
                false,
                null);
        propertyDefinitions.add(property);
    }

    public void addProperty(String propertyName, String dataType, String comment, Object defaultValue) {
        if (checkPropertyExist(propertyName)) {
            throw new GraphCreateSchemaException("there's duplicate property " + propertyName + " in type " + label);
        }
        GraphProperty property = new DefaultGraphProperty(propertyName,
                0,
                this.getDataType(StringUtils.upperCase(dataType)),
                comment,
                true,
                defaultValue);
        propertyDefinitions.add(property);
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getLabel() {
        return label;
    }

    public List<GraphProperty> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("label", this.getLabel())
                .add("propertyDefinitions", this.getPropertyDefinitions())
                .add("comment", this.getComment())
                .toString();
    }
}
