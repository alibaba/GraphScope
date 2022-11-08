package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.clause.AbstractClause;

import org.apache.calcite.rel.type.RelDataType;

/**
 * represent any node in a parser tree, including:
 * {@link IrIdentifier},
 * {@link IrLiteral},
 * {@link IrCall}.
 */
public abstract class IrNode {
    protected AbstractClause clause;

    public abstract RelDataType inferReturnType();

    public abstract void validate();
}
