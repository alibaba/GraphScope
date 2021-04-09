package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.google.protobuf.ByteString;

public class CreateVertexTypeRequest extends AbstractDdlRequest {

    private TypeDef typeDef;

    public CreateVertexTypeRequest(TypeDef typeDef) {
        super(OperationType.CREATE_VERTEX_TYPE);
        this.typeDef = typeDef;
    }

    @Override
    protected ByteString getBytes() {
        return typeDef.toDdlProto().toByteString();
    }
}
