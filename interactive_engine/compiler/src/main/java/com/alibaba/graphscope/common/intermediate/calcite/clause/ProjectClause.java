package com.alibaba.graphscope.common.intermediate.calcite.clause;

import org.apache.calcite.sql.AbstractClause;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * maintain a list of pairs, each pair consists of expression and alias,
 * and can also be denoted as a {@code SqlNode} by using {@link org.apache.calcite.sql.SqlKind#AS}.
 */
public class ProjectClause extends AbstractClause {
    private SqlNodeList projectList;

    public ProjectClause addProject(@Nullable String alias, SqlNode expr) {
        throw new NotImplementedException("");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {}
}
