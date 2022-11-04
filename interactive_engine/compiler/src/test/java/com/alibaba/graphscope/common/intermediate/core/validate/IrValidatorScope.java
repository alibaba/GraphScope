package com.alibaba.graphscope.common.intermediate.core.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

/**
 * maintain all meta for type inference and table namespace look-up in each scope.
 */
public class IrValidatorScope {
    // record mappings between node and it's inferred type
    private Map<SqlNode, RelDataType> nodeToTypeMap;
    // record mappings between alias and the corresponding table namespace
    private Map<String, TableNameSpace> aliasToNameSpaceMap;

    public void registerNameSpace(String alias, TableNameSpace nameSpace) {}

    public void registerNodeType(SqlNode node, RelDataType dataType) {}
}
