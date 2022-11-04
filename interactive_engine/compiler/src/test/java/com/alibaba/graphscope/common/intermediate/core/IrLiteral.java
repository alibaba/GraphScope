package com.alibaba.graphscope.common.intermediate.core;

import com.alibaba.graphscope.common.intermediate.core.type.IrTypeName;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * represent a constant.
 */
public class IrLiteral extends IrNode {
    // The type with which this literal was declared
    private IrTypeName typeName;
    @Nullable private Object value;

    protected IrLiteral(@Nullable Object value, IrTypeName typeName) {}

    @Override
    public RelDataType inferReturnType() {
        throw new NotImplementedException();
    }

    @Override
    public void validate() {}
}
