package com.alibaba.graphscope.common.intermediate.core.type;

import com.alibaba.graphscope.common.intermediate.core.IrCall;

/**
 * Strategy interface to check for operand types of an operator call {@link IrCall}.
 */
public interface IrOperandTypeChecker {
    /**
     * check the types of all operands in the <code>IrCall</code>
     * @param call
     * @return whether check succeeded
     */
    boolean checkOperandTypes(IrCall call);

    boolean checkOperandCount(IrCall call);
}
