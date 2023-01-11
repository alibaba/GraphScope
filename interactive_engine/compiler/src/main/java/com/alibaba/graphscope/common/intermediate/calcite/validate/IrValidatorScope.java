package com.alibaba.graphscope.common.intermediate.calcite.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.AbstractIrValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;

import java.util.Map;

/**
 * maintain meta for type inference and table namespace look-up in each scope.
 */
public class IrValidatorScope extends AbstractIrValidatorScope {
    // record mappings between node and it's inferred type
    private Map<SqlNode, RelDataType> nodeToTypeMap;

    public void registerNameSpace(String alias, IrIdentifierNameSpace nameSpace) {}

    public void registerNodeType(SqlNode node, RelDataType dataType) {}

    @Override
    public void addChild(SqlValidatorNamespace ns, String alias, boolean nullable) {}
}
