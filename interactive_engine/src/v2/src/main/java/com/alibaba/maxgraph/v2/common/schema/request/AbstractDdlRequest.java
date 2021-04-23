package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.protobuf.ByteString;

public abstract class AbstractDdlRequest {

    protected OperationType operationType;

    public AbstractDdlRequest(OperationType operationType) {
        this.operationType = operationType;
    }

    protected abstract ByteString getBytes();

    public DdlRequestBlob toBlob() {
        return new DdlRequestBlob(operationType, getBytes());
    }
}
