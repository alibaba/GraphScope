package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

public class PatternVertex {
    private Integer vertexTypeId;
    private Integer position;
    private int id;

    public PatternVertex(Integer vertexTypeId) {
        this.vertexTypeId = vertexTypeId;
        this.position = 0;
        this.id = 0;
    }

    public PatternVertex(Integer vertexTypeId, int position) {
        this.vertexTypeId = vertexTypeId;
        this.position = position;
        // TODO: update rank
        this.id = position;
    }

    public PatternVertex(Integer vertexTypeId, int position, int id) {
        this.vertexTypeId = vertexTypeId;
        this.position = position;
        this.id = id;
    }

    public Integer getVertexTypeId() {
        return vertexTypeId;
    }

    public Integer getPosition() {
        return position;
    }

    public int getId() {
        return id;
    }

    public String toString() {
        return position.toString() + "[" + vertexTypeId.toString() + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PatternVertex)) {
            return false;
        }
        PatternVertex other = (PatternVertex) o;
        // Pattern Vertex should take id into consideration, in case that vertices with same type would be ignored when adding into the pattern
        return this.vertexTypeId.equals(other.vertexTypeId) && this.position.equals(other.position);
    }
}
