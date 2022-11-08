package com.alibaba.graphscope.common.intermediate.core.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * represent all basic types from a {@code IrTypeName}
 */
public class IrBasicType extends BasicSqlType {
    public IrBasicType(RelDataTypeSystem typeSystem, IrTypeName typeName) {
        super(typeSystem, SqlTypeName.ANY);
    }
}
