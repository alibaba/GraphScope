package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

public class PatternVertex {
    private Integer vertexTypeId;
    private Integer position;

    public PatternVertex(Integer vertexTypeId) {
        this.vertexTypeId = vertexTypeId;
        this.position = 0;
    }

    public PatternVertex(Integer vertexTypeId, int position) {
        this.vertexTypeId = vertexTypeId;
        this.position = position;
    }

    public Integer getVertexTypeId() {
        return vertexTypeId;
    }

    public Integer getPosition() {
        return position;
    }

    public String toString() {
        return position.toString() + "[" + vertexTypeId.toString() + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
