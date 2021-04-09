package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class RemoveEdgeKindOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private EdgeKind edgeKind;

    public RemoveEdgeKindOperation(int partitionId, long schemaVersion, EdgeKind edgeKind) {
        super(OperationType.REMOVE_EDGE_KIND);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.edgeKind = edgeKind;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(edgeKind.toProto().toByteString())
                .build()
                .toByteString();
    }
}
