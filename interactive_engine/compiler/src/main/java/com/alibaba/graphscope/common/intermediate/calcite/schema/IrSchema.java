package com.alibaba.graphscope.common.intermediate.calcite.schema;

import org.apache.calcite.schema.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public abstract class IrSchema implements Schema {
    @Override
    public @Nullable Table getTable(String s) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return null;
    }
}
