package com.alibaba.maxgraph.v2.frontend.write;

import com.alibaba.maxgraph.v2.common.operation.EdgeId;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;

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
