package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.clause.AbstractClause;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;

/**
 * represent a call to an {@link IrOperator} operator,
 * every non-leaf-node in a parser tree is a <code>IrCall</code>.
 */
public class IrCall extends IrNode {
    private List<IrNode> operandList;
    private IrOperator irOperator;

    protected IrCall(
            AbstractClause clause,
            IrOperatorKind irOperatorKind,
            IrOperator operator,
            List<IrNode> irNodeList) {}

    public List<IrNode> getOperandList() {
        return operandList;
    }

    @Override
    public RelDataType inferReturnType() {
        throw new NotImplementedException();
    }

    @Override
    public void validate() {}
}
