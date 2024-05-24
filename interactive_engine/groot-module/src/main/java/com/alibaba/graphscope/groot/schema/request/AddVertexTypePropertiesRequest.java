package com.alibaba.graphscope.groot.schema.request;

import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.google.protobuf.ByteString;

public class AddVertexTypePropertiesRequest extends AbstractDdlRequest{

    private TypeDef typeDef;

    public AddVertexTypePropertiesRequest(TypeDef typeDef) {
        super(OperationType.ADD_VERTEX_TYPE_PROPERTIES);
        this.typeDef = typeDef;
    }

    @Override
    protected ByteString getBytes() {
        return typeDef.toDdlProto().toByteString();
    }
}
