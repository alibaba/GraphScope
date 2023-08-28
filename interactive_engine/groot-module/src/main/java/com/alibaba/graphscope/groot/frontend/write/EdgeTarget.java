package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.common.schema.wrapper.EdgeKind;
import com.alibaba.graphscope.groot.operation.EdgeId;

public class EdgeTarget {
    private EdgeKind edgeKind;
    private EdgeId edgeId;

    public EdgeTarget(EdgeKind edgeKind, EdgeId edgeId) {
        this.edgeKind = edgeKind;
        this.edgeId = edgeId;
    }

    public EdgeKind getEdgeKind() {
        return edgeKind;
    }

    public EdgeId getEdgeId() {
        return edgeId;
    }
}
