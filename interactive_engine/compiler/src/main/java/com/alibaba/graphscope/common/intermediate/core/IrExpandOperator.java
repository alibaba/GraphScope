package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.clause.type.DirectionOpt;
import com.alibaba.graphscope.common.intermediate.core.type.IrOperandTypeChecker;
import com.alibaba.graphscope.common.intermediate.core.type.IrReturnTypeInference;
import jline.internal.Nullable;
import org.apache.commons.lang3.NotImplementedException;

public class IrExpandOperator extends IrOperator {
    private DirectionOpt directionOpt;

    public IrExpandOperator(
            String name,
            DirectionOpt directionOpt,
            IrOperatorKind kind,
            @Nullable IrOperandTypeChecker operandTypeChecker,
            IrReturnTypeInference returnTypeInference) {
        super(name, kind, operandTypeChecker, returnTypeInference);
    }

    public DirectionOpt getDirectionOpt() {
        return directionOpt;
    }

    @Override
    public void validateCall(IrCall irCall) {
        throw new NotImplementedException("");
    }
}
