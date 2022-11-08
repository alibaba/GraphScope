package com.alibaba.graphscope.common.intermediate.core.clause;

import com.alibaba.graphscope.common.intermediate.core.IrNode;

import org.apache.commons.lang3.NotImplementedException;

/**
 * maintain an expression to filter.
 */
public class WhereClause extends AbstractClause {
    private IrNode expr;

    public WhereClause setFilter(IrNode expr) {
        throw new NotImplementedException();
    }
}
