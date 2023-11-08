package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class SinglePatternEdge extends PatternEdge {
    private int id;
    private EdgeTypeId edgeTypeId;
    private PatternVertex srcVertex;
    private PatternVertex dstVertex;

    public SinglePatternEdge(PatternVertex src, PatternVertex dst, EdgeTypeId edgeTypeId, int id) {
        this(src, dst, edgeTypeId, id, false, new ElementDetails());
    }

    public SinglePatternEdge(
            PatternVertex src,
            PatternVertex dst,
            EdgeTypeId edgeTypeId,
            int id,
            boolean isBoth,
            ElementDetails details) {
        super(
                isBoth,
                details,
                new EdgeIsomorphismChecker(Lists.newArrayList(edgeTypeId), isBoth, details));
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

    @Override
    public boolean isDistinct() {
        return true;
    }
}
