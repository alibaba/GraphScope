package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class AbstractClause extends SqlNode {
    public AbstractClause() {
        super(SqlParserPos.QUOTED_ZERO);
    }

    @Override
    public SqlNode clone(SqlParserPos sqlParserPos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter sqlWriter, int i, int i1) {}

    @Override
    public <R> R accept(SqlVisitor<R> sqlVisitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(@Nullable SqlNode sqlNode, Litmus litmus) {
        return false;
    }
}
