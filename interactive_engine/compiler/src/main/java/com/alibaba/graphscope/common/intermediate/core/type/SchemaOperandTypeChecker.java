package com.alibaba.graphscope.common.intermediate.core.type;

import com.alibaba.graphscope.common.intermediate.core.IrCall;
import com.alibaba.graphscope.common.intermediate.core.IrExtendOperator;
import com.alibaba.graphscope.common.intermediate.core.clause.type.DirectionOpt;
import com.alibaba.graphscope.common.intermediate.core.clause.type.ExtendOpt;

import org.apache.commons.lang3.NotImplementedException;

public class SchemaOperandTypeChecker implements IrOperandTypeChecker {
    @Override
    public boolean checkOperandTypes(IrCall call) {
        if (!checkOperandCount(call)) return false;
        IrSchemaType leftTable = (IrSchemaType) call.getOperandList().get(0).inferReturnType();
        IrSchemaType rightTable = (IrSchemaType) call.getOperandList().get(1).inferReturnType();
        IrExtendOperator operator = (IrExtendOperator) call.getIrOperator();
        return checkLeftExtendJoinRight(
                leftTable, rightTable, operator.getExtendOpt(), operator.getDirectionOpt());
    }

    // first check whether tableOpt(s) of left and right are consistent with the option of extend
    // then check whether there is a one-hop with the direction of opt between left and right
    private boolean checkLeftExtendJoinRight(
            IrSchemaType left, IrSchemaType right, ExtendOpt extendOpt, DirectionOpt directionOpt) {
        throw new NotImplementedException();
    }

    @Override
    public boolean checkOperandCount(IrCall call) {
        int operands = call.getOperandList().size();
        return operands == 2;
    }
}
