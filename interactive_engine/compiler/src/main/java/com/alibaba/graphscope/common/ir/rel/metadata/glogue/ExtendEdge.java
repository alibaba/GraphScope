package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class ExtendEdge {
    int srcVertexRank;
    EdgeTypeId edgeTypeId;
    // the direction of the extend edge
    PatternDirection direction;

    public ExtendEdge(int srcVertexRank, EdgeTypeId edgeTypeId, PatternDirection direction) {
        this.srcVertexRank = srcVertexRank;
        this.edgeTypeId = edgeTypeId;
        this.direction = direction;
    }

    public int getSrcVertexRank() {
        return srcVertexRank;
    }

    public EdgeTypeId getEdgeTypeId() {
        return edgeTypeId;
    }

    public PatternDirection getDirection() {
        return direction;
    }

    @Override
    public String toString() {
        return srcVertexRank + edgeTypeId.toString();
    }
}
