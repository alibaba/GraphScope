package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.proto.v2.PrepareDataLoadPb;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import com.google.protobuf.ByteString;

public class PrepareDataLoadOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private DataLoadTarget target;
    private long tableIdx;

    public PrepareDataLoadOperation(int partitionId, long schemaVersion, DataLoadTarget target, long tableIdx) {
        super(OperationType.PREPARE_DATA_LOAD);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.target = target;
        this.tableIdx = tableIdx;
    }

    @Override
    protected long getPartitionKey() {
        return this.partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(PrepareDataLoadPb.newBuilder()
                        .setTarget(this.target.toProto())
                        .setTableIdx(tableIdx)
                        .build().toByteString())
                .build()
                .toByteString();
    }
}
