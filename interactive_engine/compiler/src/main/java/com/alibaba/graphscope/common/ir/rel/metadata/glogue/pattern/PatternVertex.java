package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

public class PatternVertex {
    private Integer vertexTypeId;
    private Integer id;

    public PatternVertex(Integer vertexTypeId) {
        this.vertexTypeId = vertexTypeId;
        this.id = 0;
    }

    public PatternVertex(Integer vertexTypeId, int id) {
        this.vertexTypeId = vertexTypeId;
        this.id = id;
    }

    public Integer getVertexTypeId() {
        return vertexTypeId;
    }

    public Integer getId() {
        return id;
    }

    public String toString() {
        return id.toString() + "[" + vertexTypeId.toString() + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
