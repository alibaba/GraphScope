package com.alibaba.graphscope.common.intermediate.core.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.NotImplementedException;

public abstract class IrTable implements Table {
    /**
     *
     * @param relDataTypeFactory
     * @return {@link com.alibaba.graphscope.common.intermediate.core.type.IrSchemaType}
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        throw new NotImplementedException("");
    }

    @Override
    public Statistic getStatistic() {
        throw new NotImplementedException("");
    }
}
