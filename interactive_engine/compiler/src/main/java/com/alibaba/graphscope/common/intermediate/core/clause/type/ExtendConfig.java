package com.alibaba.graphscope.common.intermediate.core.clause.type;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ExtendConfig {
    private ExtendOpt extendOpt;
    private DirectionOpt directionOpt;
    @Nullable private String tableName;
    @Nullable private String alias;
}
