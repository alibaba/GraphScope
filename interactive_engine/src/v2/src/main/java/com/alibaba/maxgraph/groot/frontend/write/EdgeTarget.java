package com.alibaba.maxgraph.groot.frontend.write;

import com.alibaba.maxgraph.groot.common.operation.EdgeId;
import com.alibaba.maxgraph.groot.common.schema.EdgeKind;

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
