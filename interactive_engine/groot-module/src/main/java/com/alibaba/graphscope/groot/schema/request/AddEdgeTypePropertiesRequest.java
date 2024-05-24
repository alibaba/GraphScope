package com.alibaba.graphscope.groot.schema.request;

import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.google.protobuf.ByteString;

public class AddEdgeTypePropertiesRequest extends AbstractDdlRequest{

    private TypeDef typeDef;

    public AddEdgeTypePropertiesRequest(TypeDef typeDef) {
        super(OperationType.ADD_EDGE_TYPE_PROPERTIES);
        this.typeDef = typeDef;
    }

    @Override
    protected ByteString getBytes() {
        return typeDef.toDdlProto().toByteString();
    }
}
