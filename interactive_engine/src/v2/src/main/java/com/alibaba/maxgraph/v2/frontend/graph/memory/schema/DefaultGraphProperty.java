package com.alibaba.maxgraph.v2.frontend.graph.memory.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Default graph property in memory for testing
 */
public class DefaultGraphProperty implements GraphProperty {
    private String name;
    private int id;
    private DataType dataType;
    private String comment;
    private boolean hasDefaultValue;
    private Object defaultValue;

    public DefaultGraphProperty(String name, int id, DataType dataType) {
        this(name, id, dataType, "", false, null);
    }

    public DefaultGraphProperty(String name,
                                int id,
                                DataType dataType,
                                String comment,
                                boolean hasDefaultValue,
                                Object defaultValue) {
        if ((hasDefaultValue && defaultValue == null) || (!hasDefaultValue && defaultValue != null)) {
            throw new GraphCreateSchemaException("invalid property for hasDefaultValue[" + hasDefaultValue + "] and defaultValue[" + defaultValue + "]");
        }
        this.name = name;
        this.id = id;
        this.dataType = dataType;
        this.comment = comment;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DataType getDataType() {
        return this.dataType;
    }

    @Override
    public String getComment() {
        return this.comment;
    }

    @Override
    public boolean hasDefaultValue() {
        return this.hasDefaultValue;
    }

    @Override
    public Object getDefaultValue() {
        return this.defaultValue;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", this.getName())
                .add("id", this.getId())
                .add("dataType", this.getDataType())
                .add("comment", this.getComment())
                .add("hasDefaultValue", this.hasDefaultValue())
                .add("defaultValue", this.getDefaultValue())
                .toString();
    }
}
