package com.alibaba.graphscope.groot.operation.ddl;

import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.proto.groot.DdlOperationPb;
import com.google.protobuf.ByteString;

public class AddEdgeTypePropertiesOperation extends Operation {

    private final int partitionId;
    private final long schemaVersion;
    private final TypeDef typeDef;

    public AddEdgeTypePropertiesOperation(int partitionId, long schemaVersion, TypeDef typeDef) {
        super(OperationType.ADD_EDGE_TYPE_PROPERTIES);
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
