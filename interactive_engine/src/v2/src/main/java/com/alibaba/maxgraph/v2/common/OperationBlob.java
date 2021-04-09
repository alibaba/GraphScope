package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.proto.v2.OperationPb;
import com.alibaba.maxgraph.v2.common.operation.MarkerOperation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class OperationBlob {
    public static final OperationBlob MARKER_OPERATION_BLOB = new MarkerOperation().toBlob();

    private long partitionKey;
    private OperationType operationType;
    private ByteString dataBytes;

    public OperationBlob(long partitionKey, OperationType operationType, ByteString dataBytes) {
        this.partitionKey = partitionKey;
        this.operationType = operationType;
        this.dataBytes = dataBytes;
    }

    public static OperationBlob parseProto(OperationPb proto) {
        long partitionKey = proto.getPartitionKey();
        OperationType operationType = OperationType.parseProto(proto.getOpType());
        ByteString bytes = proto.getDataBytes();
        return new OperationBlob(partitionKey, operationType, bytes);
    }

    public long getPartitionKey() {
        return partitionKey;
    }

    public OperationPb toProto() {
        return OperationPb.newBuilder()
                .setPartitionKey(partitionKey)
                .setOpType(operationType.toProto())
                .setDataBytes(dataBytes)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationBlob that = (OperationBlob) o;

        if (partitionKey != that.partitionKey) return false;
        if (operationType != that.operationType) return false;
        return dataBytes != null ? dataBytes.equals(that.dataBytes) : that.dataBytes == null;
    }

}
