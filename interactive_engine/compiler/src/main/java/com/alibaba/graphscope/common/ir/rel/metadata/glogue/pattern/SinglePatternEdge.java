package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.Arrays;
import java.util.List;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public class SinglePatternEdge extends PatternEdge{
        private int id;
    private EdgeTypeId edgeTypeId;
    private PatternVertex srcVertex;
    private PatternVertex dstVertex;

    public SinglePatternEdge(EdgeTypeId edgeTypeId, int id) {
        this.edgeTypeId = edgeTypeId;
        this.id = id;
    }

    public SinglePatternEdge(PatternVertex src, PatternVertex dst, EdgeTypeId edgeTypeId, int id) {
        this.edgeTypeId = edgeTypeId;
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
        return Arrays.asList(edgeTypeId);
    }

    @Override
    public Integer getId() {
        return id;
    }
}
