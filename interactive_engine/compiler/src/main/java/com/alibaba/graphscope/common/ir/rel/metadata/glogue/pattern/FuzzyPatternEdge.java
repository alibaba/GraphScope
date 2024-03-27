package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

import java.util.List;

public class FuzzyPatternEdge extends PatternEdge {
    private int id;
    private List<EdgeTypeId> edgeTypeIds;
    private PatternVertex srcVertex;
    private PatternVertex dstVertex;

    public FuzzyPatternEdge(
            PatternVertex src, PatternVertex dst, List<EdgeTypeId> edgeTypeIds, int id) {
        this(src, dst, edgeTypeIds, id, false, new ElementDetails());
    }

    public FuzzyPatternEdge(
            PatternVertex src,
            PatternVertex dst,
            List<EdgeTypeId> edgeTypeIds,
            int id,
            boolean isBoth,
            ElementDetails details) {
        super(isBoth, details, new EdgeIsomorphismChecker(edgeTypeIds, isBoth, details));
        this.edgeTypeIds = edgeTypeIds;
        this.id = id;
        this.srcVertex = src;
        this.dstVertex = dst;
    }

    @Override
    public PatternVertex getSrcVertex() {
        return srcVertex;
    }

    @Override
    public PatternVertex getDstVertex() {
        return dstVertex;
    }

    @Override
    public List<EdgeTypeId> getEdgeTypeIds() {
        return edgeTypeIds;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public String toString() {
        return getSrcVertex().getId()
                + "->"
                + getDstVertex().getId()
                + "["
                + getEdgeTypeIds().toString()
                + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
