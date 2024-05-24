package com.alibaba.graphscope.groot.operation.ddl;

import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.proto.groot.DdlOperationPb;
import com.alibaba.graphscope.proto.groot.AddVertexTypePropertiesPb;
import com.google.protobuf.ByteString;

public class AddVertexTypePropertiesOperation extends Operation {

    private final int partitionId;
    private final long schemaVersion;
    private final TypeDef typeDef;
    private final long tableIdx;

    public AddVertexTypePropertiesOperation(
            int partitionId, long schemaVersion, TypeDef typeDef, long tableIdx) {
        super(OperationType.ADD_VERTEX_TYPE_PROPERTIES);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.typeDef = typeDef;
        this.tableIdx = tableIdx;
    }

    public int getPartitionId() {
        return partitionId;
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
                .setDdlBlob(
                        AddVertexTypePropertiesPb.newBuilder()
                        .setTypeDef(typeDef.toProto())
                        .setTableIdx(tableIdx)
                        .build()
                        .toByteString()
                )
                .build()
                .toByteString();
    }
}
