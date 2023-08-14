package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

public class PatternVertex {
    private Integer vertexTypeId;
    private Integer id;
    private int rank;

    public PatternVertex(Integer vertexTypeId) {
        this.vertexTypeId = vertexTypeId;
        this.id = 0;
        this.rank = 0;
    }

    public PatternVertex(Integer vertexTypeId, int id) {
        this.vertexTypeId = vertexTypeId;
        this.id = id;
        // TODO: update rank
        this.rank = id;
    }

    public PatternVertex(Integer vertexTypeId, int id, int rank) {
        this.vertexTypeId = vertexTypeId;
        this.id = id;
        this.rank = rank;
    }

    public Integer getVertexTypeId() {
        return vertexTypeId;
    }

    public Integer getId() {
        return id;
    }

    public int getRank() {
        return rank;
    }

    public String toString() {
        return id.toString() + "[" + vertexTypeId.toString() + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof PatternVertex) && (toString().equals(o.toString()));
    }
}
