package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.List;

public class FuzzyPatternVertex extends PatternVertex {

    private Integer id;
    private List<Integer> vertexTypeIds;

    public FuzzyPatternVertex(List<Integer> vertexTypeIds) {
        this(vertexTypeIds, 0);
    }

    public FuzzyPatternVertex(List<Integer> vertexTypeIds, int id) {
        this(vertexTypeIds, id, new ElementDetails());
    }

    public FuzzyPatternVertex(List<Integer> typeIds, int id, ElementDetails details) {
        super(details, new VertexIsomorphismChecker(typeIds, details));
        this.vertexTypeIds = typeIds;
        this.id = id;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public List<Integer> getVertexTypeIds() {
        return vertexTypeIds;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }
}
