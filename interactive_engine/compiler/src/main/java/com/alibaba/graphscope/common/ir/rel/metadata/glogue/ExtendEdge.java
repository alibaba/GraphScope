package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class ExtendEdge {
    // Notice that this is srcVertexId, not srcVertexPosition
    private int srcVertexId;
    private EdgeTypeId edgeTypeId;
    // the direction of the extend edge
    private PatternDirection direction;

    public ExtendEdge(int srcVertexId, EdgeTypeId edgeTypeId, PatternDirection direction) {
        this.srcVertexId = srcVertexId;
        this.edgeTypeId = edgeTypeId;
        this.direction = direction;
    }

    public int getSrcVertexId() {
        return srcVertexId;
    }

    public EdgeTypeId getEdgeTypeId() {
        return edgeTypeId;
    }

    public PatternDirection getDirection() {
        return direction;
    }

    @Override
    public String toString() {
        return srcVertexId + edgeTypeId.toString();
    }


    // // TODO:  be careful here. only validate edgeTypeId is due to the reason that
    // // we do not want to expand one ExtendStep containing ExtendEdge like [person->person, person<-person]
    // @Override
    // public boolean equals(Object obj) {
    //     if (obj instanceof ExtendEdge) {
    //         ExtendEdge other = (ExtendEdge) obj;
    //         return this.edgeTypeId.equals(other.edgeTypeId);
    //     }
    //     return false;
    // }
}
