package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;

public class VertexType extends Type implements GraphVertex {
    public int tableId;

    @Override
    public String toString() {
        return "VertexType{"
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
    public long getTableId() {
        return tableId;
    }
}
