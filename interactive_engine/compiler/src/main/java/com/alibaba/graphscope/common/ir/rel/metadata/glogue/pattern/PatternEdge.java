package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class PatternEdge {
    private Integer id;
    private int rank;
    private EdgeTypeId edgeTypeId;
    private PatternVertex srcVertex;
    private PatternVertex dstVertex;

    public PatternEdge(EdgeTypeId edgeTypeId, int id) {
        this.edgeTypeId = edgeTypeId;
        this.id = id;
        this.rank = 0;
    }

    public PatternEdge(PatternVertex src, PatternVertex dst, EdgeTypeId edgeTypeId, int id) {
        this.edgeTypeId = edgeTypeId;
        this.id = id;
        // TODO: update rank
        this.rank = id;
        this.srcVertex = src;
        this.dstVertex = dst;
    }

    public PatternVertex getSrcVertex() {
        return srcVertex;
    }

    public PatternVertex getDstVertex() {
        return dstVertex;
    }

    public String toString() {
        return srcVertex.getId() + "->" + dstVertex.getId() + "[" + edgeTypeId.toString() + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof PatternEdge) && (toString().equals(o.toString()));
    }
}
