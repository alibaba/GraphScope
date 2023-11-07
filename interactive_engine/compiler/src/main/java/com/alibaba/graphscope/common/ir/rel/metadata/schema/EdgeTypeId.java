package com.alibaba.graphscope.common.ir.rel.metadata.schema;

import org.javatuples.Triplet;

public class EdgeTypeId {
    // srcLabelId, dstLabelId, edgeLabelId
    private final Triplet<Integer, Integer, Integer> edgeType;

    public EdgeTypeId(int srcLabelId, int dstLabelId, int edgeLabelId) {
        this.edgeType = new Triplet<>(srcLabelId, dstLabelId, edgeLabelId);
    }

    public EdgeTypeId(VertexTypeId srcLabelId, VertexTypeId dstLabelId, int edgeLabelId) {
        this.edgeType = new Triplet<>(srcLabelId.vertexType, dstLabelId.vertexType, edgeLabelId);
    }

    public Integer getSrcLabelId() {
        return edgeType.getValue0();
    }

    public Integer getDstLabelId() {
        return edgeType.getValue1();
    }

    public Integer getEdgeLabelId() {
        return edgeType.getValue2();
    }

    public Triplet<Integer, Integer, Integer> getEdgeType() {
        return edgeType;
    }

    public static EdgeTypeId of(VertexTypeId sourceType, VertexTypeId targetType, int labelId) {
        return new EdgeTypeId(sourceType, targetType, labelId);
    }

    @Override
    public String toString() {
        return String.format("[%d-%d->%d]", getSrcLabelId(), getEdgeLabelId(), getDstLabelId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EdgeTypeId) {
            return this.edgeType.equals(((EdgeTypeId) obj).edgeType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.edgeType.hashCode();
    }

    @Override
    public EdgeTypeId clone() {
        return new EdgeTypeId(this.getSrcLabelId(), this.getDstLabelId(), this.getEdgeLabelId());
    }

    public int compareTo(EdgeTypeId other) {
        return this.edgeType.compareTo(other.edgeType);
    }
}
