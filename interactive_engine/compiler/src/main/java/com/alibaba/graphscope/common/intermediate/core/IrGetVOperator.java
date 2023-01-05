package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.clause.type.GetVOpt;
import com.alibaba.graphscope.common.intermediate.core.type.IrOperandTypeChecker;
import com.alibaba.graphscope.common.intermediate.core.type.IrReturnTypeInference;
import jline.internal.Nullable;
import org.apache.commons.lang3.NotImplementedException;

public class IrGetVOperator extends IrOperator{
    private GetVOpt getVOpt;

    public IrGetVOperator(
            String name,
            GetVOpt getVOpt,
            IrOperatorKind kind,
            @Nullable IrOperandTypeChecker operandTypeChecker,
            IrReturnTypeInference returnTypeInference) {
        super(name, kind, operandTypeChecker, returnTypeInference);
    }

    @Override
    public void validateCall(IrCall irCall) {
        throw new NotImplementedException("");
    }
}
