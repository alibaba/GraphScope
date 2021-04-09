package com.alibaba.maxgraph.v2.common.operation.dml;

import com.alibaba.maxgraph.proto.v2.DataOperationPb;
import com.alibaba.maxgraph.v2.common.operation.EdgeId;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class DeleteEdgeOperation extends Operation {

    private EdgeId edgeId;
    private EdgeKind edgeKind;

    public DeleteEdgeOperation(EdgeId edgeId, EdgeKind edgeKind) {
        super(OperationType.DELETE_EDGE);
        this.edgeId = edgeId;
        this.edgeKind = edgeKind;
    }

    @Override
    protected long getPartitionKey() {
        return edgeId.getSrcId().getId();
    }

    @Override
    protected ByteString getBytes() {
        DataOperationPb.Builder builder = DataOperationPb.newBuilder();
        builder.setKeyBlob(edgeId.toProto().toByteString());
        builder.setLocationBlob(edgeKind.toOperationProto().toByteString());
        return builder.build().toByteString();
    }
}
