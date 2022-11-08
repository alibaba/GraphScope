package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.clause.type.DirectionOpt;
import com.alibaba.graphscope.common.intermediate.core.clause.type.ExtendOpt;
import com.alibaba.graphscope.common.intermediate.core.type.IrOperandTypeChecker;
import com.alibaba.graphscope.common.intermediate.core.type.IrReturnTypeInference;

public class IrExtendOperator extends IrOperator {
    private ExtendOpt extendOpt;
    private DirectionOpt directionOpt;

    public IrExtendOperator(
            String name,
            ExtendOpt extendOpt,
            DirectionOpt directionOpt,
            IrOperatorKind kind,
            IrOperandTypeChecker operandTypeChecker,
            IrReturnTypeInference returnTypeInference) {
        super(name, kind, operandTypeChecker, returnTypeInference);
    }

    public ExtendOpt getExtendOpt() {
        return extendOpt;
    }

    public DirectionOpt getDirectionOpt() {
        return directionOpt;
    }
}
