package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.List;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class FuzzyPatternEdge extends PatternEdge {
    private int id;
    private List<EdgeTypeId> edgeTypeIds;
    private PatternVertex srcVertex;
    private PatternVertex dstVertex;

    public FuzzyPatternEdge(List<EdgeTypeId> edgeTypeIds, int id) {
        this.edgeTypeIds = edgeTypeIds;
        this.id = id;
    }

    public FuzzyPatternEdge(PatternVertex src, PatternVertex dst, List<EdgeTypeId> edgeTypeIds, int id) {
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
}
