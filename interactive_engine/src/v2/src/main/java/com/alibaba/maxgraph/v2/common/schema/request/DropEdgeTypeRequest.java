package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.google.protobuf.ByteString;

public class DropEdgeTypeRequest extends AbstractDdlRequest {

    private String label;

    public DropEdgeTypeRequest(String label) {
        super(OperationType.DROP_EDGE_TYPE);
        this.label = label;
    }

    @Override
    protected ByteString getBytes() {
        return TypeDef.newBuilder().setTypeEnum(TypeEnum.EDGE).setLabel(label).build().toDdlProto().toByteString();
    }
}
