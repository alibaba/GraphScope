package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.google.protobuf.ByteString;

public class RemoveEdgeKindRequest extends AbstractDdlRequest {

    private EdgeKind edgeKind;

    public RemoveEdgeKindRequest(EdgeKind edgeKind) {
        super(OperationType.REMOVE_EDGE_KIND);
        this.edgeKind = edgeKind;
    }

    @Override
    protected ByteString getBytes() {
        return edgeKind.toDdlProto().toByteString();
    }
}
