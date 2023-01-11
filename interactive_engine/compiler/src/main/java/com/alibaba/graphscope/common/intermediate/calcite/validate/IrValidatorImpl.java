package com.alibaba.graphscope.common.intermediate.calcite.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.*;

import java.util.IdentityHashMap;
import java.util.Map;

public class IrValidatorImpl extends SqlValidatorImpl {
    private IdentityHashMap<SqlNode, RelDataType> nodeToTypeMap;
    private Map<SqlNode, SqlValidatorScope> scopes;

    public IrValidatorImpl() {
        super(null, null, null, null);
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        return null;
    }

    // create new scope for current sub-query
    public void registerScope(SqlNodeList parent, SqlValidatorScope scope) {}

    public void registerNodeType(SqlNode node, RelDataType type) {}

    public void registerNameSpace(
            SqlValidatorScope scope, String alias, SqlValidatorNamespace namespace) {
        scope.addChild(namespace, alias, false);
    }

    public RelDataType getNodeType(SqlNode node) {
        return null;
    }
}
