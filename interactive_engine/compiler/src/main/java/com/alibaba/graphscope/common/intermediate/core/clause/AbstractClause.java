package com.alibaba.graphscope.common.intermediate.core.clause;

import com.alibaba.graphscope.common.intermediate.core.validate.IrValidatorScope;

/**
 * a basic type for all clauses,
 * use {@link #scope} to identify scopes in which the clause is located.
 */
public abstract class AbstractClause {
    protected IrValidatorScope scope;
}
