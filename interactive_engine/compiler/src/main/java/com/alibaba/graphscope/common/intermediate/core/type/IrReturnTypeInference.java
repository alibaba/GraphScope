package com.alibaba.graphscope.common.intermediate.core.type;

import com.alibaba.graphscope.common.intermediate.core.IrCall;

import org.apache.calcite.rel.type.RelDataType;

/**
 * Strategy interface to infer the type of an operator call from the type of the operands.
 */
public interface IrReturnTypeInference {
    /**
     *
     * @param call
     * @return inferred type
     */
    RelDataType inferReturnType(IrCall call);
}
