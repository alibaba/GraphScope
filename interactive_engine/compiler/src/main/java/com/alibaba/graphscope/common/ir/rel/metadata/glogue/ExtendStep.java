package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.List;

public class ExtendStep {
    Integer targetVertexType;
    List<ExtendEdge> extendEdges;

    public ExtendStep(Integer targetVertexType, List<ExtendEdge> extendEdges) {
        this.targetVertexType = targetVertexType;
        this.extendEdges = extendEdges;
    }

    public Integer getTargetVertexType() {
        return targetVertexType;
    }

    public List<ExtendEdge> getExtendEdges() {
        return extendEdges;
    }

    @Override
    public String toString() {
        return "ExtendStep{" +
                "targetVertexType=" + targetVertexType +
                ", extendEdges=" + extendEdges +
                '}';
    }
}
