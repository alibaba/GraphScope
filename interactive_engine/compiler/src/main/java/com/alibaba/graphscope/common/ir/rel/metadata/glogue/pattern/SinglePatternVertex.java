package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.Arrays;
import java.util.List;


public class SinglePatternVertex extends PatternVertex {
    private Integer vertexTypeId;
    private Integer id;

    public SinglePatternVertex(Integer vertexTypeId) {
        this.vertexTypeId = vertexTypeId;
        this.id = 0;
    }

    public SinglePatternVertex(Integer vertexTypeId, int id) {
        this.vertexTypeId = vertexTypeId;
        this.id = id;
    }

    @Override
    public List<Integer> getVertexTypeIds() {
        return Arrays.asList(vertexTypeId);
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
