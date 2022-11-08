package com.alibaba.graphscope.common.intermediate.core.clause.type;

import com.alibaba.graphscope.common.intermediate.core.type.TableOpt;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ScanConfig {
    private TableOpt opt;
    @Nullable private String tableName;
    @Nullable private String alias;
}
