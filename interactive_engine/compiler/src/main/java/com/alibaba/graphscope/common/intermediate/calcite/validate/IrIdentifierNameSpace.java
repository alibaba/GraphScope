package com.alibaba.graphscope.common.intermediate.calcite.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.AbstractIrNameSpace;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;

import java.util.List;

/**
 * maintain meta for a specific table or a group of tables.
 *
 * {@link #identifier} is the identifier to get tables from a global view of all tables,
 * i.e. SqlIdentifier(["PERSON"]), SqlNodeList(SqlIdentifier(["PERSON"]), SqlIdentifier(["SOFTWARE"])),
 * especially, SqlIdentifier(["*"]) represents all tables.
 *
 * {@link #tableNameSpaces} having schema for each table in {@link #identifier}.
 */
public class IrIdentifierNameSpace extends AbstractIrNameSpace {
    private SqlNode identifier;
    private List<SqlValidatorNamespace> tableNameSpaces;

    public IrIdentifierNameSpace() {}

    @Override
    protected RelDataType validateImpl(RelDataType relDataType) {
        return null;
    }

    @Override
    public RelDataType getRowType() {
        return null;
    }
}
