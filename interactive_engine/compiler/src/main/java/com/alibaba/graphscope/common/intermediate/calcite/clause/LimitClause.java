package com.alibaba.graphscope.common.intermediate.calcite.clause;

import org.apache.calcite.sql.AbstractClause;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.NotImplementedException;

public class LimitClause extends AbstractClause {
    // represent range as a two-elements list of IrLiterals
    private SqlNodeList range;

    public LimitClause setLower(int lower) {
        throw new NotImplementedException("");
    }

    public LimitClause setUpper(int upper) {
        throw new NotImplementedException("");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {}
}
