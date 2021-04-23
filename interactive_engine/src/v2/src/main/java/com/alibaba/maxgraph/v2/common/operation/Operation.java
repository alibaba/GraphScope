package com.alibaba.maxgraph.v2.common.operation;

import com.alibaba.maxgraph.v2.common.OperationBlob;
import com.google.protobuf.ByteString;

public abstract class Operation {

    protected OperationType operationType;

    public Operation(OperationType operationType) {
        this.operationType = operationType;
    }

    protected abstract long getPartitionKey();

    protected abstract ByteString getBytes();

    public OperationBlob toBlob() {
        return new OperationBlob(getPartitionKey(), operationType, getBytes());
    }
}
