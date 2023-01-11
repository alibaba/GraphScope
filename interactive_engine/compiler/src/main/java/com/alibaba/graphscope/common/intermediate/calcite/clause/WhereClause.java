package com.alibaba.graphscope.common.intermediate.calcite.clause;

import org.apache.calcite.sql.AbstractClause;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.NotImplementedException;

/**
 * maintain an expression to filter.
 */
public class WhereClause extends AbstractClause {
    private SqlNode expr;

    public WhereClause setFilter(SqlNode expr) {
        throw new NotImplementedException("");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {}
}
