package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class Property implements GraphProperty {
    public int propertyId;
    public String propertyName;
    public PropertyType propertyType;
    public String description;
    public String defaultValue;
    public Boolean nullable;

    @Override
    public String toString() {
        return "Property{"
                + "propertyId="
                + propertyId
                + ", propertyName='"
                + propertyName
                + '\''
                + ", propertyType="
                + propertyType
                + ", description='"
                + description
                + '\''
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", nullable="
                + nullable
                + '}';
    }

    @Override
    @JsonIgnore
    public int getId() {
        return propertyId;
    }

    @Override
    @JsonIgnore
    public String getName() {
        return propertyName;
    }

    @Override
    @JsonIgnore
    public DataType getDataType() {
        return propertyType.toImpl();
    }

    @Override
    @JsonIgnore
    public String getComment() {
        return description;
    }

    @Override
    @JsonIgnore
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    @Override
    @JsonIgnore
    public Object getDefaultValue() {
        return defaultValue;
    }
}
