package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.google.protobuf.ByteString;

public class DropVertexTypeRequest extends AbstractDdlRequest {

    private String label;

    public DropVertexTypeRequest(String label) {
        super(OperationType.DROP_VERTEX_TYPE);
        this.label = label;
    }

    @Override
    protected ByteString getBytes() {
        return TypeDef.newBuilder().setLabel(label).build().toDdlProto().toByteString();
    }
}
