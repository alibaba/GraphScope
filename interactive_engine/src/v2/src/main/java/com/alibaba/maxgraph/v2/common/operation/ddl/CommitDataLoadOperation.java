package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.CommitDataLoadPb;
import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.proto.v2.PrepareDataLoadPb;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class CommitDataLoadOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private CommitDataLoadPb proto;

    public CommitDataLoadOperation(int partitionId, long schemaVersion, CommitDataLoadPb proto) {
        super(OperationType.COMMIT_DATA_LOAD);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.proto = proto;
    }

    @Override
    protected long getPartitionKey() {
        return this.partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(proto.toByteString())
                .build()
                .toByteString();
    }
}
