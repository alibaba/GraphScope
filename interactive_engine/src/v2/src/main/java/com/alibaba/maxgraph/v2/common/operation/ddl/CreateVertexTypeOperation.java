package com.alibaba.maxgraph.v2.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.CreateVertexTypePb;
import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.google.protobuf.ByteString;

public class CreateVertexTypeOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private TypeDef typeDef;
    private long tableIdx;

    public CreateVertexTypeOperation(int partitionId, long schemaVersion, TypeDef typeDef, long tableIdx) {
        super(OperationType.CREATE_VERTEX_TYPE);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.typeDef = typeDef;
        this.tableIdx = tableIdx;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getSchemaVersion() {
        return schemaVersion;
    }

    public TypeDef getTypeDef() {
        return typeDef;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(CreateVertexTypePb.newBuilder()
                        .setTypeDef(typeDef.toProto())
                        .setTableIdx(tableIdx)
                        .build().toByteString())
                .build()
                .toByteString();
    }
}
