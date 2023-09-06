package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class ExtendEdge {
    private int srcVertexOrder;
    private EdgeTypeId edgeTypeId;
    // the direction of the extend edge
    private PatternDirection direction;

    public ExtendEdge(int srcVertexOrder, EdgeTypeId edgeTypeId, PatternDirection direction) {
        this.srcVertexOrder = srcVertexOrder;
        this.edgeTypeId = edgeTypeId;
        this.direction = direction;
    }

    public int getSrcVertexOrder() {
        return srcVertexOrder;
    }

    public EdgeTypeId getEdgeTypeId() {
        return edgeTypeId;
    }

    public PatternDirection getDirection() {
        return direction;
    }

    @Override
    public String toString() {
        return srcVertexOrder + edgeTypeId.toString();
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
