package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class PatternEdge {
    private int id;
    private EdgeTypeId edgeTypeId;
    private PatternVertex srcVertex;
    private PatternVertex dstVertex;

    public PatternEdge(EdgeTypeId edgeTypeId, int id) {
        this.edgeTypeId = edgeTypeId;
        this.id = id;
    }

    public PatternEdge(PatternVertex src, PatternVertex dst, EdgeTypeId edgeTypeId, int id) {
        this.edgeTypeId = edgeTypeId;
        this.id = id;
        this.srcVertex = src;
        this.dstVertex = dst;
    }

    public PatternVertex getSrcVertex() {
        return srcVertex;
    }

    public PatternVertex getDstVertex() {
        return dstVertex;
    }

    public EdgeTypeId getEdgeTypeId() {
        return edgeTypeId;
    }

    public int getId() {
        return id;
    }

    public String toString() {
        return srcVertex.getPosition() + "->" + dstVertex.getPosition() + "[" + edgeTypeId.toString() + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PatternEdge)) {
            return false;
        }
        PatternEdge other = (PatternEdge) o;
        return this.edgeTypeId.equals(other.edgeTypeId);
    }
}
