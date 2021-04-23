package com.alibaba.maxgraph.v2.common.operation;

import com.google.protobuf.ByteString;

public class MarkerOperation extends Operation {

    public MarkerOperation() {
        super(OperationType.MARKER);
    }

    @Override
    protected long getPartitionKey() {
        return -1;
    }

    @Override
    protected ByteString getBytes() {
        return ByteString.EMPTY;
    }
}
