package com.alibaba.graphscope.common.intermediate.core.type;

import com.alibaba.graphscope.common.intermediate.core.IrCall;
import com.alibaba.graphscope.common.intermediate.core.IrNode;

import org.apache.calcite.rel.type.RelDataType;

public class SchemaReturnTypeInference implements IrReturnTypeInference {
    @Override
    public RelDataType inferReturnType(IrCall call) {
        IrNode rightTable = call.getOperandList().get(1);
        return rightTable.inferReturnType();
    }
}
