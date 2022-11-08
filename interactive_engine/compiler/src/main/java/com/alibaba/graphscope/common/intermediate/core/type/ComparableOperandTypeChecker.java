package com.alibaba.graphscope.common.intermediate.core.type;

import com.alibaba.graphscope.common.intermediate.core.IrCall;
import com.alibaba.graphscope.common.intermediate.core.IrNode;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;

public class ComparableOperandTypeChecker implements IrOperandTypeChecker {
    private final RelDataTypeComparability requiredComparability;

    public ComparableOperandTypeChecker(RelDataTypeComparability requiredComparability) {
        this.requiredComparability = requiredComparability;
    }

    @Override
    public boolean checkOperandTypes(IrCall call) {
        boolean b = true;
        for (IrNode operand : call.getOperandList()) {
            RelDataType type = operand.inferReturnType();
            if (!checkType(type)) {
                b = false;
                break;
            }
        }
        return b;
    }

    @Override
    public boolean checkOperandCount(IrCall call) {
        int operands = call.getOperandList().size();
        return operands == 2;
    }

    private boolean checkType(RelDataType type) {
        if (type.getComparability().ordinal() < requiredComparability.ordinal()) {
            return false;
        } else {
            return true;
        }
    }
}
