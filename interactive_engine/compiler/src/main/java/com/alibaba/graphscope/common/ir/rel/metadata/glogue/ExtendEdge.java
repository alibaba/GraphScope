package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PathExpandRange;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

public class ExtendEdge {
    // the src vertex to extend the edge
    private int srcVertexOrder;
    // the types of the extend edge
    private final List<EdgeTypeId> edgeTypeIds;
    // the direction of the extend edge
    private PatternDirection direction;
    // the weight of the extend edge, which indicates the cost to expand the edge.
    private Double weight;
    // to denote the range of the path expand operator
    private final @Nullable PathExpandRange range;

    public ExtendEdge(int srcVertexOrder, EdgeTypeId edgeTypeId, PatternDirection direction) {
        this(srcVertexOrder, edgeTypeId, direction, null);
    }

    public ExtendEdge(
            int srcVertexOrder, List<EdgeTypeId> edgeTypeIds, PatternDirection direction) {
        this(srcVertexOrder, edgeTypeIds, direction, null);
    }

    public ExtendEdge(
            int srcVertexOrder, EdgeTypeId edgeTypeId, PatternDirection direction, Double weight) {
        this(srcVertexOrder, ImmutableList.of(edgeTypeId), direction, weight, null);
    }

    public ExtendEdge(
            int srcVertexOrder,
            List<EdgeTypeId> edgeTypeIds,
            PatternDirection direction,
            Double weight) {
        this(srcVertexOrder, edgeTypeIds, direction, weight, null);
    }

    public ExtendEdge(
            int srcVertexOrder,
            List<EdgeTypeId> edgeTypeIds,
            PatternDirection direction,
            Double weight,
            PathExpandRange range) {
        this.srcVertexOrder = srcVertexOrder;
        this.edgeTypeIds = edgeTypeIds;
        this.direction = direction;
        this.weight = weight;
        this.range = range;
    }

    public int getSrcVertexOrder() {
        return srcVertexOrder;
    }

    public EdgeTypeId getEdgeTypeId() {
        return edgeTypeIds.get(0);
    }

    public List<EdgeTypeId> getEdgeTypeIds() {
        return Collections.unmodifiableList(edgeTypeIds);
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

    public @Nullable PathExpandRange getRange() {
        return range;
    }

    @Override
    public String toString() {
        return srcVertexOrder
                + (edgeTypeIds.size() == 1 ? edgeTypeIds.get(0).toString() : edgeTypeIds.toString())
                + ": "
                + weight;
    }
}
