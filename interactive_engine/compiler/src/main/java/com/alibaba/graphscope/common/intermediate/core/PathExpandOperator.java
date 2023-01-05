package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.clause.type.DirectionOpt;
import com.alibaba.graphscope.common.intermediate.core.clause.type.GetVOpt;
import com.alibaba.graphscope.common.intermediate.core.type.IrOperandTypeChecker;
import com.alibaba.graphscope.common.intermediate.core.type.IrReturnTypeInference;
import jline.internal.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.NotImplementedException;
import org.javatuples.Pair;

public class PathExpandOperator extends IrOperator {
    private DirectionOpt directionOpt;
    private GetVOpt getVOpt;
    private Pair<Integer, Integer> range;

    public PathExpandOperator(String name,
                              IrOperatorKind kind,
                              DirectionOpt directionOpt,
                              GetVOpt getVOpt,
                              @Nullable IrOperandTypeChecker operandTypeChecker,
                              @Nullable IrReturnTypeInference returnTypeInference) {
        super(name, kind, operandTypeChecker, returnTypeInference);
    }

    // validate two operands
    @Override
    public void validateCall(IrCall irCall) {
        throw new NotImplementedException("");
    }

    @Override
    public RelDataType inferReturnTypeFromCall(IrCall irCall) {
        throw new NotImplementedException("");
    }
}
