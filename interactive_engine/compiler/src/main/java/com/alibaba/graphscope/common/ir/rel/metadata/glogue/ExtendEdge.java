package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class ExtendEdge {
    // the src vertex to extend the edge
    private int srcVertexOrder;
    // the type of the extend edge
    private EdgeTypeId edgeTypeId;
    // the direction of the extend edge
    private PatternDirection direction;
    // the weight of the extend edge, which indicates the cost to expand the edge.
    private Double weight;

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

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Double getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return srcVertexOrder + edgeTypeId.toString() + ": " + weight;
    }
}
