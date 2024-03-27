package com.alibaba.graphscope.common.ir.rel.metadata.schema;

public class VertexTypeId {
    int vertexType;

    public VertexTypeId(int vertexType) {
        this.vertexType = vertexType;
    }

    public int getVertexTypeId() {
        return vertexType;
    }

    public static VertexTypeId of(int labelId) {
        return new VertexTypeId(labelId);
    }

    @Override
    public String toString() {
        return String.format("%d", getVertexTypeId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VertexTypeId) {
            return this.vertexType == ((VertexTypeId) obj).vertexType;
        }
        return false;
    }
}
