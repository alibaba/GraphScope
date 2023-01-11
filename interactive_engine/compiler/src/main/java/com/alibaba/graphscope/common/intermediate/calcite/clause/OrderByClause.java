package com.alibaba.graphscope.common.intermediate.calcite.clause;

import com.alibaba.graphscope.common.intermediate.calcite.clause.type.OrderOpt;

import org.apache.calcite.sql.AbstractClause;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.NotImplementedException;

/**
 * maintain a list of pairs, each pair consists of expression (variable) and opt (DESC or ASC),
 * and can also be denoted as a {@code SqlNode} by using {@link org.apache.calcite.sql.SqlKind#DESCENDING}.
 */
public class OrderByClause extends AbstractClause {
    private SqlNodeList orderByList;

    public OrderByClause addOrderBy(SqlNode expr, OrderOpt opt) {
        throw new NotImplementedException("");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {}
}
