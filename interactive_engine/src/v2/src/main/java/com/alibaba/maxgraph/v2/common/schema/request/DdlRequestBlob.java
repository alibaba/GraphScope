package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.proto.v2.DdlRequestPb;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.google.common.base.Objects;
import com.google.protobuf.ByteString;

public class DdlRequestBlob {

    private OperationType operationType;
    private ByteString bytes;

    public DdlRequestBlob(OperationType operationType, ByteString bytes) {
        this.operationType = operationType;
        this.bytes = bytes;
    }

    public static DdlRequestBlob parseProto(DdlRequestPb proto) {
        OperationType operationType = OperationType.parseProto(proto.getOpType());
        return new DdlRequestBlob(operationType, proto.getDdlBytes());
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public ByteString getBytes() {
        return bytes;
    }

    public DdlRequestPb toProto() {
        return DdlRequestPb.newBuilder()
                .setOpType(operationType.toProto())
                .setDdlBytes(bytes)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DdlRequestBlob that = (DdlRequestBlob) o;
        return operationType == that.operationType &&
                Objects.equal(bytes, that.bytes);
    }

}
