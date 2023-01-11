package com.alibaba.graphscope.common.intermediate.calcite.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.validate.AbstractIrNameSpace;

public class TableNameSpace extends AbstractIrNameSpace {
    private Table table;

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        return null;
    }
}
