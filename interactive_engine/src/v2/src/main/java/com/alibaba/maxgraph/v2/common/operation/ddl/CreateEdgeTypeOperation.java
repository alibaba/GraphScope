package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.google.protobuf.ByteString;

public class CreateEdgeTypeOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private TypeDef typeDef;

    public CreateEdgeTypeOperation(int partitionId, long schemaVersion, TypeDef typeDef) {
        super(OperationType.CREATE_EDGE_TYPE);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.typeDef = typeDef;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(typeDef.toProto().toByteString())
                .build()
                .toByteString();
    }
}
