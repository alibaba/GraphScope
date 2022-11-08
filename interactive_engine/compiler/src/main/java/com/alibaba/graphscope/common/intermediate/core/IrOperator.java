package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.type.IrOperandTypeChecker;
import com.alibaba.graphscope.common.intermediate.core.type.IrReturnTypeInference;

import jline.internal.Nullable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

/**
 * help to denote a expression.
 */
public class IrOperator {
    private String name;
    private IrOperatorKind kind;
    @Nullable private IrOperandTypeChecker operandTypeChecker;
    @Nullable private IrReturnTypeInference returnTypeInference;

    public IrOperator(
            String name,
            IrOperatorKind kind,
            @Nullable IrOperandTypeChecker operandTypeChecker,
            @Nullable IrReturnTypeInference returnTypeInference) {}

    public RelDataType inferReturnTypeFromCall(IrCall irCall) {
        if (returnTypeInference != null) {
            return returnTypeInference.inferReturnType(irCall);
        }
        throw new NotImplementedException();
    }

    public void validateCall(IrCall irCall) {
        List<IrNode> operands = irCall.getOperandList();
        for (IrNode each : operands) {
            each.validate();
        }
        if (operandTypeChecker != null) {
            operandTypeChecker.checkOperandTypes(irCall);
        }
    }
}
