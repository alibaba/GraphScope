package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.google.protobuf.ByteString;

public class AddEdgeKindRequest extends AbstractDdlRequest {

    private EdgeKind edgeKind;

    public AddEdgeKindRequest(EdgeKind edgeKind) {
        super(OperationType.ADD_EDGE_KIND);
        this.edgeKind = edgeKind;
    }

    @Override
    protected ByteString getBytes() {
        return edgeKind.toDdlProto().toByteString();
    }
}
