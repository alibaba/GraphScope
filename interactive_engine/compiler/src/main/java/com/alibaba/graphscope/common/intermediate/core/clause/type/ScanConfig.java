package com.alibaba.graphscope.common.intermediate.core.clause.type;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ScanConfig {
    private ScanOpt opt;
    @Nullable private String tableName;
    @Nullable private String alias;
}
