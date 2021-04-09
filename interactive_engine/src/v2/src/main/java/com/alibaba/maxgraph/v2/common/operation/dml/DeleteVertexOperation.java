package com.alibaba.maxgraph.v2.common.operation.dml;

import com.alibaba.maxgraph.proto.v2.DataOperationPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.operation.VertexId;
import com.google.protobuf.ByteString;

public class DeleteVertexOperation extends Operation {

    private VertexId vertexId;
    private LabelId labelId;

    public DeleteVertexOperation(VertexId vertexId, LabelId labelId) {
        super(OperationType.DELETE_VERTEX);
        this.vertexId = vertexId;
        this.labelId = labelId;
    }

    @Override
    protected long getPartitionKey() {
        return vertexId.getId();
    }

    @Override
    protected ByteString getBytes() {
        DataOperationPb.Builder builder = DataOperationPb.newBuilder();
        builder.setKeyBlob(vertexId.toProto().toByteString());
        builder.setLocationBlob(labelId.toProto().toByteString());
        return builder.build().toByteString();
    }
}
