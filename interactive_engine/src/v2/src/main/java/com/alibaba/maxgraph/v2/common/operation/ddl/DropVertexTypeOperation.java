package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class DropVertexTypeOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private LabelId labelId;

    public DropVertexTypeOperation(int partitionId, long schemaVersion, LabelId labelId) {
        super(OperationType.DROP_VERTEX_TYPE);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.labelId = labelId;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(labelId.toProto().toByteString())
                .build()
                .toByteString();
    }
}
