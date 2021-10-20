package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.operation.EdgeId;
import com.alibaba.graphscope.groot.schema.EdgeKind;

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
