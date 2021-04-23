package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.AddEdgeKindPb;
import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class AddEdgeKindOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private EdgeKind edgeKind;
    private long tableIdx;

    public AddEdgeKindOperation(int partitionId, long schemaVersion, EdgeKind edgeKind, long tableIdx) {
        super(OperationType.ADD_EDGE_KIND);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.edgeKind = edgeKind;
        this.tableIdx = tableIdx;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(AddEdgeKindPb.newBuilder()
                        .setEdgeKind(edgeKind.toProto())
                        .setTableIdx(tableIdx)
                        .build().toByteString())
                .build()
                .toByteString();
    }
}
